import hashlib
import json
from collections import Counter

import pyarrow as pa

from .common import VERSION, dump, export, literal


class Components:
    def __init__(self):
        self.parent = {}
        self.size = {}

    def find(self, value):
        if value not in self.parent:
            self.parent[value] = value
            self.size[value] = 1
        root = value
        while self.parent[root] != root:
            root = self.parent[root]
        while value != root:
            previous = value
            value = self.parent[value]
            self.parent[previous] = root
        return root

    def union(self, a, b):
        a, b = self.find(a), self.find(b)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size[b]


def build_blocks(db, output):
    db.execute("""CREATE TABLE block_members AS SELECT DISTINCT
      sha256(identifier_type||':'||namespace||':'||value) block_id,
      identifier_type,namespace,value,source,entity_id
      FROM identifiers WHERE usable AND namespace IS NOT NULL AND value IS NOT NULL""")
    db.execute("""CREATE TABLE evidence_blocks AS WITH by_source AS (
      SELECT block_id,identifier_type,namespace,value,source,count(*) n
      FROM block_members GROUP BY ALL)
      SELECT block_id,identifier_type,namespace,value,sum(n)::BIGINT entity_count,
        count(*) sources,((sum(n)*sum(n)-sum(n*n))/2)::BIGINT cross_source_pair_count
      FROM by_source GROUP BY block_id,identifier_type,namespace,value HAVING count(*)>=2""")
    db.execute(
        """CREATE TABLE shared_members AS SELECT m.* FROM block_members m JOIN evidence_blocks b USING(block_id)"""
    )
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM evidence_blocks",
        output / "candidate_identifier_blocks.parquet",
    )
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM shared_members",
        output / "candidate_block_members.parquet",
    )
    export(
        db,
        f"""SELECT b.block_id,i.assertion_id,i.entity_id,i.source_record_id,i.source_sha256,
      {literal(VERSION)} processing_rule FROM identifiers i JOIN evidence_blocks b
      ON i.identifier_type=b.identifier_type AND i.namespace=b.namespace AND i.value=b.value WHERE i.usable""",
        output / "candidate_block_assertions.parquet",
    )


def connected_components(db):
    graph = Components()
    reader = db.execute(
        "SELECT block_id,entity_id FROM shared_members ORDER BY block_id,entity_id"
    ).to_arrow_reader(8192)
    previous = first = None
    for batch in reader:
        for block_id, entity_id in zip(
            batch.column(0).to_pylist(), batch.column(1).to_pylist()
        ):
            if block_id != previous:
                first, previous = entity_id, block_id
                graph.find(first)
            else:
                graph.union(first, entity_id)
    minima = {}
    for entity in graph.parent:
        root = graph.find(entity)
        minima[root] = min(minima.get(root, entity), entity)
    entity_ids, component_ids = [], []
    for entity in sorted(graph.parent):
        entity_ids.append(entity)
        component_ids.append(
            hashlib.sha256(
                (VERSION + ":component:" + minima[graph.find(entity)]).encode()
            ).hexdigest()
        )
    table = pa.table(
        {
            "entity_id": pa.array(entity_ids, type=pa.string()),
            "candidate_component_id": pa.array(component_ids, type=pa.string()),
        }
    )
    db.register("component_input", table)
    db.execute("CREATE TABLE component_members AS SELECT * FROM component_input")
    db.unregister("component_input")


def assess_components(db, output):
    db.execute("""CREATE TABLE component_entities AS SELECT m.*,e.* EXCLUDE(entity_id)
      FROM component_members m JOIN entities e USING(entity_id)""")
    db.execute("""CREATE TABLE structured_names AS SELECT candidate_component_id,
      name_script(first_name||' '||last_name) script,
      count(distinct first_name) first_variants,count(distinct last_name) last_variants,
      count(distinct middle_name) middle_variants,min(first_name) first_name,min(last_name) last_name
      FROM component_entities WHERE first_name IS NOT NULL AND last_name IS NOT NULL
      AND length(replace(first_name,'.',''))>1 AND length(replace(last_name,'.',''))>1 GROUP BY 1,2""")
    db.execute("""CREATE TABLE name_checks AS WITH fields AS (
      SELECT candidate_component_id,'first' field,first_name AS "value" FROM component_entities
      UNION ALL SELECT candidate_component_id,'last',last_name FROM component_entities
      UNION ALL SELECT candidate_component_id,'middle',middle_name FROM component_entities),
      variants AS (SELECT candidate_component_id,field,name_script(value) script,count(distinct value) n
      FROM fields WHERE value IS NOT NULL AND length(replace(value,'.',''))>1 GROUP BY 1,2,3)
      SELECT candidate_component_id,bool_or(n>1) conflicting_structured_names FROM variants GROUP BY 1""")
    db.execute("""CREATE TABLE generic_name_checks AS SELECT e.candidate_component_id,
      bool_or(CASE WHEN s.first_name IS NOT NULL THEN NOT list_has_all(string_split(e.full_name,' '),
        string_split(s.first_name||' '||s.last_name,' ')) ELSE false END) generic_name_unresolved,
      count(distinct CASE WHEN s.first_name IS NULL THEN e.full_name END) unsupported_full_name_variants
      FROM component_entities e LEFT JOIN structured_names s ON e.candidate_component_id=s.candidate_component_id
        AND name_script(e.full_name)=s.script WHERE e.full_name IS NOT NULL GROUP BY 1""")
    db.execute("""CREATE TABLE birth_constraints AS WITH dated AS (
      SELECT candidate_component_id,entity_id,birth_dates_json FROM component_entities
      WHERE json_array_length(birth_dates_json)>0), dates AS (
      SELECT d.candidate_component_id,d.entity_id,json_extract_string(j.value,'$') birth_date
      FROM dated d,json_each(d.birth_dates_json) j), support AS (
      SELECT candidate_component_id,birth_date,count(distinct entity_id) n FROM dates GROUP BY 1,2)
      SELECT d.candidate_component_id,count(distinct d.entity_id) known_birth_entities,
        (SELECT max(n) FROM support s WHERE s.candidate_component_id=d.candidate_component_id) max_date_support
      FROM dated d GROUP BY 1""")
    db.execute("""CREATE TABLE component_counts AS SELECT candidate_component_id,count(*) entities,
      count(distinct source) sources,count(*) FILTER(WHERE entity_kind IN ('native_user','native_profile')) native_profiles,
      count(distinct document_key) known_documents,
      count(distinct CASE WHEN upper(sex) IN ('M','MALE') THEN 'M' WHEN upper(sex) IN ('F','FEMALE') THEN 'F' END) known_sexes,
      count(distinct CASE WHEN name_script(coalesce(first_name||' '||last_name,full_name)) IN ('latin','cyrillic')
        THEN name_script(coalesce(first_name||' '||last_name,full_name)) END) name_scripts
      FROM component_entities GROUP BY 1""")
    db.execute("""CREATE TABLE group_checks AS SELECT c.*,
      coalesce(n.conflicting_structured_names,false) conflicting_structured_names,
      coalesce(g.generic_name_unresolved,false) generic_name_unresolved,
      coalesce(g.unsupported_full_name_variants,0) unsupported_full_name_variants,
      coalesce(b.known_birth_entities,0) known_birth_entities,
      coalesce(b.max_date_support,0) max_date_support,
      list_filter([
        CASE WHEN c.known_documents>1 THEN 'different_known_documents' END,
        CASE WHEN c.known_sexes>1 THEN 'different_known_sexes' END,
        CASE WHEN c.name_scripts>1 THEN 'cross_script_names_not_resolved' END,
        CASE WHEN n.conflicting_structured_names THEN 'different_structured_names' END,
        CASE WHEN g.generic_name_unresolved THEN 'generic_name_format_or_value_unresolved' END,
        CASE WHEN g.unsupported_full_name_variants>1 THEN 'generic_name_variants_without_structured_anchor' END,
        CASE WHEN b.known_birth_entities>b.max_date_support THEN 'disjoint_birth_date_constraints' END
      ],x->x IS NOT NULL) reasons
      FROM component_counts c LEFT JOIN name_checks n USING(candidate_component_id)
      LEFT JOIN generic_name_checks g USING(candidate_component_id)
      LEFT JOIN birth_constraints b USING(candidate_component_id)""")
    db.execute(f"""CREATE TABLE preliminary_groups AS SELECT * EXCLUDE(reasons),to_json(reasons) reasons_json,
      CASE WHEN len(reasons)=0 THEN 'provisional_consistent' ELSE 'held_for_review' END status,
      CASE WHEN len(reasons)=0 THEN candidate_component_id END provisional_group_id,
      {literal(VERSION)} processing_rule FROM group_checks""")
    db.execute(f"""CREATE TABLE assignments AS SELECT e.entity_id,e.source,e.entity_kind,
      m.candidate_component_id,g.provisional_group_id,
      coalesce(g.status,'unlinked') status,e.source_record_id,e.source_sha256,
      {literal(VERSION)} processing_rule FROM entities e LEFT JOIN component_members m USING(entity_id)
      LEFT JOIN preliminary_groups g USING(candidate_component_id)""")
    assert (
        db.execute("SELECT count(*) FROM assignments").fetchone()[0]
        == db.execute("SELECT count(*) FROM entities").fetchone()[0]
    )
    assert (
        db.execute(
            "SELECT count(*) FROM preliminary_groups WHERE provisional_group_id IS NOT NULL AND reasons_json!='[]'"
        ).fetchone()[0]
        == 0
    )
    for label, table in [
        ("preliminary_groups", "preliminary_groups"),
        ("entity_assignments", "assignments"),
    ]:
        export(db, f"SELECT * FROM {table}", output / f"{label}.parquet")
    return (
        db.execute(
            'SELECT status,count(*) AS "groups",sum(entities) entities FROM preliminary_groups GROUP BY status ORDER BY status'
        )
        .fetchdf()
        .to_dict("records")
    )


def alternatives(db, output):
    # These names are a review frame outside exact identifiers, never an automatic identity key.
    db.execute("""CREATE OR REPLACE TABLE name_members AS SELECT e.entity_id,e.source,e.first_name,e.last_name,
      a.candidate_component_id,sha256('name:'||e.first_name||':'||e.last_name) block_id
      FROM entities e JOIN assignments a USING(entity_id)
      WHERE e.first_name IS NOT NULL AND e.last_name IS NOT NULL
      AND length(replace(e.first_name,'.',''))>1 AND length(replace(e.last_name,'.',''))>1""")
    db.execute("""CREATE OR REPLACE TABLE outside_name_blocks AS SELECT block_id,count(*) entities,count(distinct source) sources,
      count(distinct coalesce(candidate_component_id,entity_id)) distinct_components
      FROM name_members GROUP BY block_id HAVING count(distinct source)>1
      AND count(distinct coalesce(candidate_component_id,entity_id))>1""")
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM outside_name_blocks",
        output / "outside_identifier_name_blocks.parquet",
    )
    export(
        db,
        f"SELECT n.*,{literal(VERSION)} processing_rule FROM name_members n JOIN outside_name_blocks b USING(block_id)",
        output / "outside_identifier_name_members.parquet",
    )
    # Cross-namespace matches remain candidates, even when the names agree.
    db.execute("""CREATE OR REPLACE TABLE namespace_candidates AS SELECT
      sha256('namespace:'||x.assertion_id||':'||j.assertion_id) case_id,
      x.entity_id xml_entity_id,j.entity_id json_entity_id,
      x.assertion_id xml_assertion_id,j.assertion_id json_assertion_id,
      x.namespace xml_program,j.namespace other_program,
      xe.first_name=je.first_name AND xe.last_name=je.last_name exact_name_agreement
      FROM identifiers x JOIN identifiers j ON x.value=j.value AND x.identifier_type='loyalty'
      AND j.identifier_type='loyalty' AND x.source='xml' AND j.source='json' AND x.usable AND j.usable
      AND x.namespace<>j.namespace
      JOIN entities xe ON xe.entity_id=x.entity_id JOIN entities je ON je.entity_id=j.entity_id
      """)
    export(
        db,
        f"SELECT *, 'requires_namespace_validation' status,{literal(VERSION)} processing_rule FROM namespace_candidates",
        output / "program_namespace_candidates.parquet",
    )
    db.execute("""CREATE OR REPLACE TABLE namespace_pairs AS SELECT xml_program,other_program,
      count(*) assertion_pairs,count(distinct xml_assertion_id) xml_card_assertions,
      count(*) FILTER(WHERE exact_name_agreement) exact_name_pairs
      FROM namespace_candidates GROUP BY xml_program,other_program""")
    export(
        db,
        f"SELECT *, 'requires_namespace_validation' status,{literal(VERSION)} processing_rule FROM namespace_pairs",
        output / "program_namespace_evidence.parquet",
    )
    db.execute("""CREATE OR REPLACE TABLE ticket_members AS SELECT source,entity_id,observation_id,
      sha256('ticket:'||ticket_number||':'||cast(date AS VARCHAR)||':'||flight) block_id
      FROM observations WHERE clean_text(ticket_number) IS NOT NULL AND date IS NOT NULL AND flight IS NOT NULL""")
    db.execute("""CREATE OR REPLACE TABLE ticket_blocks AS SELECT block_id,count(*) observations,count(distinct source) sources
      FROM ticket_members GROUP BY block_id HAVING count(distinct source)>1""")
    export(
        db,
        f"SELECT *, 'observation_candidate_only' status,{literal(VERSION)} processing_rule FROM ticket_blocks",
        output / "ticket_observation_blocks.parquet",
    )
    export(
        db,
        f"SELECT m.*,{literal(VERSION)} processing_rule FROM ticket_members m JOIN ticket_blocks b USING(block_id)",
        output / "ticket_observation_members.parquet",
    )
    query = """SELECT o.source,unit,coalesce(a.status,'unlinked') linkage_status,count(*) observations,
      count(distinct o.duplicate_key) selected_payloads FROM observations o JOIN assignments a USING(entity_id)
      GROUP BY o.source,unit,linkage_status"""
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM ({query})",
        output / "observation_linkage_sensitivity.parquet",
    )


def review_sample(db, output, limit=30):
    db.execute("""CREATE TABLE review_frame AS SELECT candidate_component_id case_id,
      'component' case_unit,status stratum,entities members,reasons_json FROM preliminary_groups
      UNION ALL SELECT block_id,'outside_identifier_name_block','outside_identifier_candidates',entities,'[]'
      FROM outside_name_blocks
      UNION ALL SELECT case_id,'profile_pair','program_namespace_candidates',2,'[]'
      FROM namespace_candidates""")
    db.execute(f"""CREATE TABLE review_selection AS SELECT *,
      count(*) OVER(PARTITION BY stratum) frame_cases,
      row_number() OVER(PARTITION BY stratum ORDER BY sha256(case_id||':review-v1')) sample_rank
      FROM review_frame QUALIFY sample_rank<={int(limit)}""")
    export(
        db,
        f"SELECT *,{literal(VERSION)} processing_rule FROM review_selection",
        output / "review_sampling.parquet",
    )
    export(
        db,
        f"""SELECT case_id,case_unit,NULL::VARCHAR reviewer_label,NULL::VARCHAR annotator,
      NULL::VARCHAR evidence_reference,NULL::VARCHAR notes,{literal(VERSION)} processing_rule
      FROM review_selection""",
        output / "review_labels.parquet",
    )
    export(
        db,
        f"""SELECT s.case_id,e.*,{literal(VERSION)} review_rule
      FROM review_selection s JOIN component_entities e ON s.case_id=e.candidate_component_id
      UNION ALL BY NAME SELECT s.case_id,e.*,{literal(VERSION)} review_rule
      FROM review_selection s JOIN name_members n ON s.case_id=n.block_id JOIN entities e USING(entity_id)
      UNION ALL BY NAME SELECT s.case_id,e.*,{literal(VERSION)} review_rule
      FROM review_selection s JOIN namespace_candidates n USING(case_id)
      JOIN entities e ON e.entity_id=n.xml_entity_id OR e.entity_id=n.json_entity_id""",
        output / "review_case_members.parquet",
    )
    dump(
        output / "review_protocol.json",
        {
            "unit": "component or outside-identifier candidate block, not a random sample of all person pairs",
            "sampling": "deterministic hash order, up to 30 cases per stratum; ranks and frame sizes retained",
            "labels": "left empty; request independent evidence, annotate ambiguity explicitly",
            "precision": None,
            "recall": None,
            "limitations": "These case reviews cannot directly estimate pair precision or global recall. No independent truth labels exist.",
        },
    )


def run_matching(db, output):
    existing = {row[0] for row in db.execute("SHOW TABLES").fetchall()}
    if "shared_members" not in existing:
        build_blocks(db, output)
    if "component_members" not in existing:
        connected_components(db)
    if "assignments" not in existing:
        groups = assess_components(db, output)
    else:
        groups = (
            db.execute(
                'SELECT status,count(*) AS "groups",sum(entities) entities FROM preliminary_groups GROUP BY status ORDER BY status'
            )
            .fetchdf()
            .to_dict("records")
        )
    alternatives(db, output)
    review_sample(db, output)
    reasons = Counter()
    for (raw,) in db.execute("SELECT reasons_json FROM preliminary_groups").fetchall():
        reasons.update(json.loads(raw))
    result = {
        "groups": groups,
        "hold_reasons": dict(reasons),
        "shared_identifier_blocks": db.execute(
            "SELECT count(*) FROM evidence_blocks"
        ).fetchone()[0],
        "outside_identifier_name_blocks": db.execute(
            "SELECT count(*) FROM outside_name_blocks"
        ).fetchone()[0],
        "program_namespace_candidate_pairs": db.execute(
            "SELECT count(*) FROM namespace_candidates"
        ).fetchone()[0],
        "ticket_observation_blocks": db.execute(
            "SELECT count(*) FROM ticket_blocks"
        ).fetchone()[0],
        "unlinked_entities": db.execute(
            "SELECT count(*) FROM assignments WHERE status='unlinked'"
        ).fetchone()[0],
        "precision": None,
        "recall": None,
        "processing_rule": VERSION,
    }
    dump(output / "matching_summary.json", result)
    return result
