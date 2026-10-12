from importlib import import_module

understand_documents_for_company = import_module(
	".understander", __package__
).understand_documents_for_company

__all__ = ["understand_documents_for_company"]