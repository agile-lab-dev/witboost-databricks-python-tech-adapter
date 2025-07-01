def get_use_case_template_id(use_case_template_id: str) -> str:
    """Extracts the base template ID from a full versioned ID string."""
    return use_case_template_id.rsplit(":", 1)[0].replace('"', "").strip()
