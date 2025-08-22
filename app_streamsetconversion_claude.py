import streamlit as st
from anthropic import Anthropic
import re, json

st.set_page_config(page_title="StreamSets to Databricks (Claude)", layout="wide")

# ---- Global styling (compact + themed buttons across all stages) ----
st.markdown("""
<style>
  .block-container { padding-top: 2rem; padding-bottom: 2rem; }
  .stTextInput>div>div>input { font-size: 16px; }
  .stTextArea textarea { font-size: 14px; }
  div.stButton > button {
    background: #2563EB !important;
    color: #FFFFFF !important;
    border: 1px solid #1E3A8A !important;
    padding: 0.35rem 0.9rem !important;
    font-size: 0.95rem !important;
    border-radius: 6px !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.06);
  }
  div.stButton > button:hover {
    background: #1D4ED8 !important;
    border-color: #1E40AF !important;
  }
</style>
""", unsafe_allow_html=True)

st.title("🔄 StreamSets to Databricks Migration Assistant")

# === Sidebar: API & Model ===
with st.sidebar:
    st.header("🔐 Claude API")
    api_key = st.text_input("API Key", type="password")
    default_model = "claude-3-5-sonnet-20241022"
    model_name = st.text_input("Model", value=default_model, help="Override if needed")
    client = Anthropic(api_key=api_key) if api_key else None
    stage = st.radio("📌 Select Stage", ["Stage 1: Parse & Visualize", "Stage 2: Databricks Alignment", "Stage 3: Generate Notebook"])

# ---- Session State ----
defaults = {
    "json_content": "",
    "stage1_output_visible": "",
    "stage1_additional_prompts": "",
    "stage1_docs_text": "",
    "stage2_prefill": {},
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ---- Helpers ----
def load_base_prompt():
    try:
        with open("prompt.txt", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return (
            "You are a data pipeline expert helping to convert StreamSets Transformer pipelines to Databricks notebooks. "
            "Follow the three-staged approach: Stage 1 (Parse & Visualize), Stage 2 (Databricks Alignment), "
            "Stage 3 (Generate Notebook). Respect constraints: target table may appear as a source but is used only "
            "for SCD2 merge; ignore dedup logic; produce clear mappings."
        )
base_prompt = load_base_prompt()

def read_small_text(uploaded_file, max_chars=20000):
    try:
        raw = uploaded_file.read()
        text = raw.decode("utf-8", errors="ignore")
        if len(text) > max_chars:
            text = text[:max_chars]
            st.info("Attached documentation truncated to 20,000 characters.")
        return text
    except Exception:
        return ""

def extract_stage2_prefill(full_text: str) -> (dict, str):
    """Extract hidden prefill JSON after the marker and return (prefill_dict, visible_text_without_json)."""
    if not full_text:
        return {}, full_text
    # Look for our unique marker followed by a fenced json block
    m = re.search(r"^===STAGE2_PREFILL_JSON===\s*```json\s*(\{.*?\})\s*```", full_text, re.S | re.M | re.I)
    if not m:
        return {}, full_text
    json_str = m.group(1)
    try:
        data = json.loads(json_str)
    except Exception:
        data = {}
    # Remove the marker + block from the visible text
    visible = re.sub(r"^===STAGE2_PREFILL_JSON===\s*```json\s*\{.*?\}\s*```", "", full_text, flags=re.S | re.M | re.I).rstrip()
    return data, visible

# ----------------------
# Stage 1
# ----------------------
if stage == "Stage 1: Parse & Visualize":
    st.markdown("""
### Stage 1: Parse & Visualize

**Goal**  
Turn the StreamSets JSON into a clear picture of the pipeline: true sources, joins, filters, transformations, and the text-based visual flow.
""")

    st.subheader("📁 Upload StreamSets Pipeline in JSON")
    uploaded_json = st.file_uploader("Choose your StreamSets JSON file", type=["json"])

    additional_prompts = st.text_area(
        "Additional Prompts (optional)",
        value=st.session_state.stage1_additional_prompts,
        height=120,
        help="Clarifications, edge cases, or constraints for Stage 1."
    )
    docs_file = st.file_uploader(
        "📎 Attach Documentation (optional, text-based files)",
        type=["txt", "md", "json", "yaml", "yml", "csv"]
    )

    docs_text = st.session_state.stage1_docs_text
    if docs_file is not None:
        docs_text = read_small_text(docs_file, max_chars=20000)

    if uploaded_json is not None:
        st.session_state.json_content = uploaded_json.read().decode("utf-8")

    left1, _ = st.columns([1, 9])
    with left1:
        run_stage1 = st.button("🚀 Run Stage 1", key="run_stage1_btn")

    if client and run_stage1:
        if not st.session_state.json_content:
            st.error("Please upload the StreamSets JSON.")
        else:
            st.session_state.stage1_additional_prompts = additional_prompts
            st.session_state.stage1_docs_text = docs_text or ""

            p = [
                base_prompt,
                "\n\nNow execute **Stage 1** using the JSON below.\n",
                "```json\n", st.session_state.json_content, "\n```"
            ]
            if additional_prompts.strip():
                p.extend(["\n\n### Additional Prompts\n", additional_prompts.strip()])
            if (docs_text or "").strip():
                p.extend(["\n\n### Attached Documentation (verbatim)\n```text\n", docs_text.strip(), "\n```"])

            # Request a HIDDEN JSON handoff after a unique marker.
            p.extend([
                "\n\nAt the very end of your response, AFTER a line containing exactly:\n",
                "===STAGE2_PREFILL_JSON===\n",
                "output ONLY a fenced JSON block with these exact keys. ",
                "Use strings; if unknown, use an empty string. Do not explain this block.\n",
                "```json\n",
                "{\n",
                "  \"target_table_name\": \"\",\n",
                "  \"primary_keys\": \"\",\n",
                "  \"business_keys\": \"\",\n",
                "  \"foreign_keys\": \"\",\n",
                "  \"audit_columns\": \"\",\n",
                "  \"target_table_structure\": \"\",\n",
                "  \"source_table_realignment\": \"\",\n",
                "  \"source_to_target_mapping\": \"\",\n",
                "  \"foreign_key_resolution\": \"\",\n",
                "  \"flow_design\": \"\"\n",
                "}\n",
                "```\n"
            ])
            prompt = "".join(p)

            with st.spinner("🧠 Migration Assistant is parsing and visualizing..."):
                try:
                    resp = client.messages.create(
                        model=model_name, max_tokens=3500, temperature=0.3,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    full = resp.content[0].text
                    prefill, visible = extract_stage2_prefill(full)

                    # Save prefill for Stage 2; save visible text to show the user
                    st.session_state.stage2_prefill = prefill or {}
                    st.session_state.stage1_output_visible = visible

                    st.success("✅ Stage 1 completed.")
                    # Show the visible portion only
                    st.code(st.session_state.stage1_output_visible, language="markdown")
                except Exception as e:
                    st.error(f"Stage 1 failed: {e}")

# ----------------------
# Stage 2
# ----------------------
elif stage == "Stage 2: Databricks Alignment":
    st.markdown("""
### Stage 2: Databricks Alignment

**Goal**  
Align Stage 1 findings with your Databricks Landscape: formalize keys, schema, source realignment, and the end-to-end transformation flow design.
""")

    st.subheader("⚙️ Provide Databricks Table Mapping Details")

    if st.session_state.stage1_output_visible:
        with st.expander("🔎 View Stage 1 Summary", expanded=False):
            st.text_area("Stage 1 Result", st.session_state.stage1_output_visible, height=160, disabled=True)

    pref = st.session_state.get("stage2_prefill", {})

    col1, col2 = st.columns(2)
    with col1:
        target_table_name = st.text_input("Target Table Name:", value=pref.get("target_table_name",""), key="stage2_target_table_name", help="Catalog.schema.table")
        business_keys = st.text_input("Business Keys", value=pref.get("business_keys",""), key="stage2_business_keys", help="Business Keys : Comma Seperated.")
        foreign_keys = st.text_input("Foreign Keys", value=pref.get("foreign_keys",""), key="stage2_foreign_keys", help="Business Keys : Comma Seperated.")
        audit_cols = st.text_input("Audit Columns", value=pref.get("audit_columns",""), key="stage2_audit_cols", help="Comma-separated list (e.g., meta_CreatedDate, meta_ModifiedDate, meta_SourceSystem, iscurrent, Version, etc).")
        source_realignment = st.text_area("Source Table Realignment (Streamsets Source → Databricks Source Table)", value=pref.get("source_table_realignment",""), key="stage2_source_realignment", height=140, help="One per line: Streamsets_Source => Catalog.schema.table [alias] or JSON object.")
    with col2:
        primary_keys = st.text_area("Primary Keys", value=pref.get("primary_keys",""), key="stage2_primary_keys", height=100, help="Primary Key : Column Name and derivation logic.")
        target_table_struct = st.text_area("Target Table Structure (DDL / JSON)", value=pref.get("target_table_structure",""), key="stage2_target_table_struct", height=140, help="Column Names including data types, nullability.")
        st_mapping = st.text_area("Source → Target Column Mapping ", value=pref.get("source_to_target_mapping",""), key="stage2_st_mapping", height=160, help="target_col ← source.table.col [transform] | ")
        fk_resolution = st.text_area("Foreign Key Resolution.", value=pref.get("foreign_key_resolution",""), key="stage2_fk_resolution", height=100, help="Provide Join conditions")
        flow_design = st.text_area("Transformation Flow Design (overview)", value=pref.get("flow_design",""), key="stage2_flow_design", height=140, help="High-level narrative of the flow (order of operations, dependencies).")

    left2, _ = st.columns([1, 9])
    with left2:
        run_stage2 = st.button("🚀 Run Stage 2", key="run_stage2_btn")

    # Always show the latest Stage 2 result if present (persisted across reruns)
    if st.session_state.get("stage2_visible"):
        st.markdown("#### Stage 2 Result (last run)")
        st.code(st.session_state["stage2_visible"], language="markdown")

    if client and run_stage2:
        if not st.session_state.json_content:
            st.error("Stage 2 requires the original StreamSets JSON from Stage 1.")
        else:
            prompt_parts = [
                base_prompt,
                "\n\nNow execute **Stage 2: Databricks Alignment** using the following inputs.\n",
                "### Target Table Name\n", (target_table_name or "").strip(), "\n",
                "### Primary Keys\n", (primary_keys or "").strip(), "\n",
                "### Business Keys\n", (business_keys or "").strip(), "\n",
                "### Foreign Keys\n", (foreign_keys or "").strip(), "\n",
                "### Target Table Structure (DDL / JSON)\n```text\n", (target_table_struct or "").strip(), "\n```\n",
                "### Audit Columns\n", (audit_cols or "").strip(), "\n",
                "### Source Table Realignment (Streamsets Source → Databricks Source Table)\n```text\n", (source_realignment or "").strip(), "\n```\n",
                "### Source → Target Column Mapping\n```text\n", (st_mapping or "").strip(), "\n```\n",
                "### Foreign Key Resolution\n```text\n", (fk_resolution or "").strip(), "\n```\n",
                "### Transformation Flow Design (overview)\n```text\n", (flow_design or "").strip(), "\n```\n",
                "\n---\n",
                "### Original StreamSets JSON\n```json\n", st.session_state.json_content, "\n```\n"
            ]
            if st.session_state.stage1_additional_prompts.strip():
                prompt_parts.extend(["\n**Carry-over Additional Prompts:**\n", st.session_state.stage1_additional_prompts.strip(), "\n"])
            if st.session_state.stage1_docs_text.strip():
                prompt_parts.extend(["\n**Carry-over Attached Documentation (verbatim):**\n```text\n", st.session_state.stage1_docs_text.strip(), "\n```\n"])

            stage2_prompt = "".join(prompt_parts)

            with st.spinner("🧠 Migration Assistant is aligning with Databricks..."):
                try:
                    resp = client.messages.create(
                        model=model_name, max_tokens=3500, temperature=0.3,
                        messages=[{"role": "user", "content": stage2_prompt}],
                    )
                    stage2_output = resp.content[0].text
                    st.session_state["stage2_output_text"] = stage2_output

                    st.session_state["stage2_data"] = {
                    "target_table_name": target_table_name or "",
                    "primary_keys": (primary_keys or "").strip(),
                    "business_keys": (business_keys or "").strip(),
                    "foreign_keys": (foreign_keys or "").strip(),
                    "audit_columns": (audit_cols or "").strip(),
                    "target_table_structure": (target_table_struct or "").strip(),
                    "source_table_realignment": (source_realignment or "").strip(),
                    "source_to_target_mapping": (st_mapping or "").strip(),
                    "foreign_key_resolution": (fk_resolution or "").strip(),
                    "flow_design": (flow_design or "").strip(),
                    }

                    prefill, visible = extract_stage2_prefill(stage2_output)

                    st.success("✅ Stage 2 completed.")
                    st.code(visible, language="markdown")
                    st.download_button("💾 Download Documentation", visible, file_name="Documentation.txt")
                except Exception as e:
                    st.error(f"Stage 2 failed: {e}")

# ----------------------
# Stage 3
# ----------------------
elif stage == "Stage 3: Generate Notebook":
    st.markdown("""
### Stage 3: Generate Notebook

**Goal**  
Generate a production-ready Databricks notebook that implements the aligned design.
""")

    st.subheader("Generate Final Databricks Notebook")
    notebook_context = st.text_area("🧠 Optional: Additional Context or Constraints for Notebook", height=150)

    left3, _ = st.columns([1, 9])
    with left3:
        run_stage3 = st.button("🚀 Generate Notebook", key="run_stage3_btn")

    stage2_text = st.session_state.get("stage2_output_text", "")
    stage2_data = st.session_state.get("stage2_data", {})

    if client and run_stage3:
        if not st.session_state.json_content:
            st.error("Please complete Stage 1 first to load the StreamSets JSON.")
        else:
            final_prompt = base_prompt + "\n\nNow execute **Stage 3**. Generate a full Databricks notebook based on the StreamSets JSON and Stage 2 Finalised Target & Keys."
            if notebook_context.strip():
                final_prompt += f"\n\n### Additional context\n{notebook_context.strip()}\n"

            if st.session_state.stage1_additional_prompts.strip():
                final_prompt += f"\n**Carry-over Additional Prompts:**\n{st.session_state.stage1_additional_prompts.strip()}\n"

            final_prompt += f"\n### Original StreamSets JSON\n```json\n{st.session_state.json_content}\n```"

            # final_prompt += "".join([
            #                         "\n### Finalised Target & Keys (from Stage 2)\n",
            #                         f"- Target Table Name:\n```text\n{stage2_data.get('target_table_name','')}\n```\n",
            #                         f"- Finalised Primary Keys:\n```text\n{stage2_data.get('primary_keys','')}\n```\n",
            #                         f"- Finalised Business Keys:\n```text\n{stage2_data.get('business_keys','')}\n```\n",
            #                         f"- Finalised Foreign Keys:\n```text\n{stage2_data.get('foreign_keys','')}\n```\n",
            #                         f"- Finalised Audit Columns:\n```text\n{stage2_data.get('audit_columns','')}\n```\n",
            #                         "\n### Finalised Target Table Structure (from Stage 2)\n",
            #                         f"```text\n{stage2_data.get('target_table_structure','')}\n```\n",
            #                         "\n### Finalised Source Table Realignment (from Stage 2)\n",
            #                         f"```text\n{stage2_data.get('source_table_realignment','')}\n```\n",
            #                         "\n### Finalised Source-to-Target Column Mappings (from Stage 2)\n",
            #                         f"```text\n{stage2_data.get('source_to_target_mapping','')}\n```\n",
            #                         "\n### Finalised Foreign Key Resolution (from Stage 2)\n",
            #                         f"```text\n{stage2_data.get('foreign_key_resolution','')}\n```\n",
            #                         "\n### Finalised Transformation Flow Design (from Stage 2)\n",
            #                         f"```text\n{stage2_data.get('flow_design','')}\n```\n",
            #                     ])

            
            final_prompt += f"\n### Stage 2 Consolidated Narrative (for your reference—use for code, not re-analysis)\n```markdown\n{stage2_text}\n```\n"

            with st.spinner("🛠️ Migration Assistant is generating notebook..."):
                try:
                    resp = client.messages.create(
                        model=model_name, max_tokens=4000, temperature=0.3,
                        messages=[{"role": "user", "content": final_prompt}],
                    )
                    nb = resp.content[0].text
                    st.success("✅ Notebook generation complete.")
                    st.code(nb, language="python")
                    st.download_button("💾 Download Notebook", nb, file_name="generated_notebook.py")
                except Exception as e:
                    st.error(f"Notebook generation failed: {e}")
