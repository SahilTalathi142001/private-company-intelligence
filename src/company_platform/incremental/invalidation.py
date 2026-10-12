class InvalidationEngine:
    def __init__(self, pipeline_registry):
        self.registry = pipeline_registry

    def affected(self, component_id):
        return self.registry.affected_downstream(component_id)

    def closure(self, component_id):
        return [component_id] + self.affected(component_id)

    def contains(self, component_id, target):
        return target in set(self.closure(component_id))
