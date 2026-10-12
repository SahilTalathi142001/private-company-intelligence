from importlib import import_module

extract_documents_for_company = import_module(
	f"{__name__}.extractor"
).extract_documents_for_company

__all__ = ["extract_documents_for_company"]