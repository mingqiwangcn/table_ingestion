import util
from context_window import ContextWindow
from serial_schema import SchemaSerializer

class CompressSerializer(SchemaSerializer):
    def __init__(self):
        super().__init__()

    def preprocess_other(self, table_data):
        return 
