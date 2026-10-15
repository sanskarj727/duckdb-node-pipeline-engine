import duckdb
from collections import defaultdict, deque
from src.element.node_factory import NodeFactory
from src.element.nodes import SourceNode
import reactivex as rx
from reactivex import operators as ops
from datetime import datetime

class Project:
    def __init__(self, pipeline_config):
        self.scribble_data = pipeline_config
        self.port_data = {}
        self.downstream_ports = defaultdict(list)
        self.id_to_action = {}
        self.duckdb_con = duckdb.connect(database=":memory:")
        self.factory = None

    def build_node_factory(self):
        flow = self.scribble_data["dataFlows"]["flow"]
        schema_dict = flow["schemaDictionary"]
        actions = flow["flowModel"]["actions"]
        action_types = set(a["actionType"] for a in actions)
        self.factory = NodeFactory(schema_dict, enabled_types=action_types)

    def build_graph_and_counts(self):
        self.downstream_ports = defaultdict(list)
        flow_model = self.scribble_data["dataFlows"]["flow"]["flowModel"]
        actions = flow_model["actions"]
        connections = flow_model["connections"]
        action_inputs_decl = {a["id"]: set(ip["id"] for ip in a.get("inputs", [])) for a in actions}
        required_inputs = {aid: len(ports) for aid, ports in action_inputs_decl.items()}
        
        for conn in connections:
            src_a = conn["source"]["action"]
            src_p = conn["source"].get("output") or conn["source"].get("port")
            tgt_a = conn["target"]["action"]
            tgt_p = conn["target"].get("input") or conn["target"].get("port")
            self.downstream_ports[(src_a, src_p)].append((tgt_a, tgt_p))
        return required_inputs, action_inputs_decl

    def _tap_preview(self, obs, label, limit=10):
        def _show(table_name):
            try:
                df = self.duckdb_con.execute(f"from {table_name} limit {limit}").fetch_df()
                print(f"\n[PREVIEW][{label}] {table_name}")
                print(df)
            except Exception:
                pass
        obs.pipe(ops.take(1)).subscribe(on_next=_show)
        return obs

    def run(self):
        self.build_node_factory()
        actions = self.scribble_data["dataFlows"]["flow"]["flowModel"]["actions"]
        self.id_to_action = {a["id"]: a for a in actions}
        self.port_data = {}
        required_inputs, action_inputs_decl = self.build_graph_and_counts()
        queue = deque()

        port_to_sourceinfo = {}
        for ds in self.scribble_data.get("dataSources", []):
            node_id = ds["id"]
            ds_ports = []
            for conn in self.scribble_data["dataFlows"]["flow"]["flowModel"]["connections"]:
                if conn["source"]["action"] == node_id:
                    port_id = conn["source"].get("output") or conn["source"].get("port")
                    ds_ports.append(port_id)
            if node_id in self.id_to_action:
                ds_ports += [p["id"] for p in self.id_to_action[node_id].get("outputs", [])]
            ds_ports = set(filter(None, ds_ports))
            for port_id in ds_ports:
                port_to_sourceinfo[(node_id, port_id)] = ds

        for (node_id, port_id), ds in port_to_sourceinfo.items():
            conn = ds.get("providerInfo", {}).get("connectionInfo", {})
            csv_path = conn.get("filePath") or ds["name"]
            table_name = ds["name"].replace(".", "_")
            file_type = conn.get("fileType", "csv")
            delimiter = conn.get("delimiter", ",")
            config = {"fileType": file_type, "delimiter": delimiter}

            source_node = SourceNode(config, self.duckdb_con, csv_path, table_name, file_type, delimiter)
            print(f"Query {node_id}: {source_node.get_query()}")

            start = datetime.now()
            obs = source_node.create_observable()
            obs = obs.pipe(ops.publish(), ops.ref_count())
            # preview for source
            obs = self._tap_preview(obs, f"SOURCE:{node_id}")
            end = datetime.now()
            elapsed_ms = (end - start).total_seconds() * 1000.0
            print(
                f"\n[TIME][SOURCE] node={node_id} "
                f"start={start.isoformat(timespec='microseconds')} "
                f"end={end.isoformat(timespec='microseconds')} "
                f"elapsed_ms={elapsed_ms:.2f}"
            )

            try:
                print(f"Source schema: {table_name}: ")
                desc_df = self.duckdb_con.execute(f"desc {table_name}").fetch_df()
                print(desc_df)
                print(f"\nSource table: {table_name}")
                src_df = self.duckdb_con.execute(f"from {table_name}").fetch_df()
                print(src_df)
            except Exception as e:
                print(f"[WARN] Could not print source '{table_name}': {e}")

            self.port_data[(node_id, port_id)] = obs
            queue.append((node_id, port_id))

        if not self.port_data:
            print("Error: Source missing")
            return

        executed = {a["id"]: False for a in actions}
        pending_inputs = defaultdict(dict)

        while queue:
            src_a, src_p = queue.popleft()
            for tgt_a, tgt_p in self.downstream_ports.get((src_a, src_p), []):
                self.port_data[(tgt_a, tgt_p)] = self.port_data[(src_a, src_p)]

                if tgt_a in required_inputs:
                    decl_ports = action_inputs_decl.get(tgt_a, set())
                    if tgt_p in decl_ports and required_inputs[tgt_a] > 0:
                        required_inputs[tgt_a] -= 1

                act = self.id_to_action.get(tgt_a)
                if not act:
                    continue

                if act.get("inputs"):
                    pending_inputs[tgt_a][tgt_p] = self.port_data.get((tgt_a, tgt_p))

                if required_inputs.get(tgt_a, 0) != 0 or executed.get(tgt_a, False):
                    continue

                cfg_any = act.get("config", {})
                if isinstance(cfg_any, list):
                    config = cfg_any[0] if cfg_any else {}
                elif isinstance(cfg_any, dict):
                    config = cfg_any
                else:
                    config = {}

                node_type = act["actionType"]
                node = self.factory.create(node_type, config, dbcon=self.duckdb_con)

                input_ports = act.get("inputs", [])
                in_obs = [self.port_data.get((tgt_a, ip["id"])) for ip in input_ports]
                if None in in_obs and input_ports:
                    continue

                def preview_sql_single(first_table_name):
                    try:
                        return node.get_query(first_table_name)
                    except Exception as e:
                        return f"get_query error: {e}"

                def preview_sql_binary(lname, rname):
                    try:
                        return node.get_query(lname, rname)
                    except Exception as e:
                        return f"get_query error: {e}"

                if node_type in ("merge", "join") and len(in_obs) == 2:
                    left_obs, right_obs = in_obs[0], in_obs[1]
                    rx.combine_latest(
                        left_obs.pipe(ops.take(1)),
                        right_obs.pipe(ops.take(1)),
                    ).subscribe(
                        on_next=lambda pair: print(
                            f"Query {tgt_a}: {preview_sql_binary(pair[0], pair[1])}"
                        )
                    )

                    start = datetime.now()
                    obs = node.create_observable(left_obs, right_obs)
                    obs = obs.pipe(ops.publish(), ops.ref_count())
                    # preview for merge/join
                    obs = self._tap_preview(obs, f"{node_type.upper()}:{tgt_a}")
                    end_holder = {"end": None}

                    def _on_next(_):
                        end_holder["end"] = datetime.now()
                        elapsed = (end_holder["end"] - start).total_seconds() * 1000.0
                        stage = "MERGE" if node_type == "merge" else "JOIN"
                        print(
                            f"\n[TIME][{stage}] node={tgt_a} "
                            f"start={start.isoformat(timespec='microseconds')} "
                            f"end={end_holder['end'].isoformat(timespec='microseconds')} "
                            f"elapsed_ms={elapsed:.2f}"
                        )

                    obs.pipe(ops.take(1)).subscribe(on_next=_on_next)
                else:
                    if input_ports:
                        first_obs = in_obs[0]
                        first_obs.pipe(ops.take(1)).subscribe(
                            on_next=lambda tn: print(
                                f"\nQuery {tgt_a}: {preview_sql_single(tn)}"
                            )
                        )

                        start = datetime.now()
                        obs = (
                            node.create_observable(*in_obs)
                            if len(in_obs) > 1
                            else node.create_observable(in_obs[0])
                        )
                        obs = obs.pipe(ops.publish(), ops.ref_count())
                        # preview for single/multi input nodes
                        obs = self._tap_preview(obs, f"{node_type.upper()}:{tgt_a}")
                        end_holder = {"end": None}

                        def _on_next(_):
                            end_holder["end"] = datetime.now()
                            elapsed = (end_holder["end"] - start).total_seconds() * 1000.0
                            print(
                                f"\n[TIME][{node_type.upper()}] node={tgt_a} "
                                f"start={start.isoformat(timespec='microseconds')} "
                                f"end={end_holder['end'].isoformat(timespec='microseconds')} "
                                f"elapsed_ms={elapsed:.2f}"
                            )

                        obs.pipe(ops.take(1)).subscribe(on_next=_on_next)
                    else:
                        print(f"\nQuery {tgt_a}: {node.get_query(None)}")
                        start = datetime.now()
                        obs = node.create_observable()
                        
                        obs = self._tap_preview(obs, f"{node_type.upper()}:{tgt_a}")
                        end_holder = {"end": None}

                        def _on_next(_):
                            end_holder["end"] = datetime.now()
                            elapsed = (end_holder["end"] - start).total_seconds() * 1000.0
                            print(
                                f"\n[TIME][{node_type.upper()}] node={tgt_a} "
                                f"start={start.isoformat(timespec='microseconds')} "
                                f"end={end_holder['end'].isoformat(timespec='microseconds')} "
                                f"elapsed_ms={elapsed:.2f}"
                            )

                        obs.pipe(ops.take(1)).subscribe(on_next=_on_next)

                for out in act.get("outputs", []):
                    self.port_data[(tgt_a, out["id"])] = obs
                    queue.append((tgt_a, out["id"]))

                executed[tgt_a] = True

        # Determine final ports as outputs of terminal actions
        if not actions:
            return

        outgoing = {
            conn["source"]["action"]
            for conn in self.scribble_data["dataFlows"]["flow"]["flowModel"]["connections"]
        }
        terminal_action_ids = [a["id"] for a in actions if a["id"] not in outgoing]

        # Fallback: if every action has outgoing edges, use last action as terminal
        if not terminal_action_ids:
            terminal_action_ids = [actions[-1]["id"]]

        final_ports = []
        for act in actions:
            if act["id"] in terminal_action_ids:
                for o in act.get("outputs", []):
                    final_ports.append((act["id"], o["id"]))

        for aid, pid in final_ports:
            obs = self.port_data.get((aid, pid))
            if not obs:
                continue

            def on_next(table_name):
                df = self.duckdb_con.execute(f"from {table_name}").fetch_df()
                print("\n")
                print(df)

            def on_error(_):
                pass

            def on_completed():
                pass

            obs.subscribe(on_next=on_next, on_error=on_error, on_completed=on_completed)

