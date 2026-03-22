"""
Metadata Service
Handles database metadata queries (schemas, tables, views)
"""

from db.snowflake import get_db


class MetadataService:
    """Service for database metadata operations"""
    
    def __init__(self, config_file):
        self.db = get_db(config_file)
    
    def get_database_info(self):
        """Get database name and connection info"""
        return {
            'database': self.db.database,
            'status': 'connected' if self.db.conn else 'disconnected'
        }
    
    def get_schemas(self):
        """Get list of all schemas"""
        return self.db.get_all_schemas()
    
    def get_schema_structure(self):
        """Get complete schema structure"""
        return self.db.get_schema_structure()
    
    def get_schema_summary(self):
        """Get summary of all schemas with counts"""
        structure = self.get_schema_structure()
        summary = {
            'database': self.db.database,
            'total_schemas': len(structure),
            'total_tables': sum(s['table_count'] for s in structure.values()),
            'total_views': sum(s['view_count'] for s in structure.values()),
            'schemas': structure
        }
        return summary
