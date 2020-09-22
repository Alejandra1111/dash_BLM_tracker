
class Factory():
    def __init__(self):
        self._tools = {}

    def register_tool(self, key, tool):
        self._tools[key] = tool

    def get_tool(self, key):
        tool = self._tools.get(key)
        if not tool:
            raise ValueError(key)
        return tool() 
