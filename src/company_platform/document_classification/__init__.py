from importlib import import_module


def classify_documents_for_company(*args, **kwargs):
	"""Load and call the document classifier on demand."""
	classifier = import_module(".classifier", __package__)
	return classifier.classify_documents_for_company(*args, **kwargs)

__all__ = ["classify_documents_for_company"]