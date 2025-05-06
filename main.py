import re
import os
import logging

from collections import defaultdict
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, inspect
from typing import Dict, List, Set


class DatabaseAnalyzer:
    def __init__(self, url: str):
        self.url = url
        self.engine = create_engine(url)
        self.metadata = self._get_all_metadata()
        self.inspector = inspect(self.engine)
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.relation_patterns = {
            'id_pattern': re.compile(r'^(?:id_|id)(\w+)$', re.IGNORECASE),
            'fk_pattern': re.compile(r'^(?:fk_|fk)(\w+)$', re.IGNORECASE),
            'table_id_pattern': re.compile(r'^(\w+)_(?:id|cd|codigo)$', re.IGNORECASE),
            'ref_pattern': re.compile(r'^(?:ref_|reference_)(\w+)$', re.IGNORECASE),
            'cod_pattern': re.compile(r'^(?:cod_|codigo_)(\w+)$', re.IGNORECASE),
            'num_pattern': re.compile(r'^(?:num_|numero_)(\w+)$', re.IGNORECASE)
        }

    def _get_all_metadata(self) -> MetaData:
        """Obtém todos os metadados do banco de dados."""
        try:
            metadata = MetaData()
            metadata.reflect(bind=self.engine)
            return metadata
        except Exception as e:
            self.logger.error(f"Erro ao obter metadados: {str(e)}")
            raise

    def _normalize_table_name(self, name: str) -> str:
        """Normaliza o nome da tabela para comparação."""
        name = re.sub(r'^(dim_|tb_|tbl_|tab_|fact_|fato_)', '', name.lower())
        name = re.sub(r'(s|es|is)$', '', name)
        return name.replace('_', '')

    def _get_all_table_variations(self, table_name: str) -> Set[str]:
        """Gera variações possíveis do nome da tabela."""
        variations = set()
        base_name = self._normalize_table_name(table_name)
        

        variations.add(base_name)
        variations.add(base_name + 's') 
        variations.add(base_name + 'es')  
        
  
        variations.add(re.sub(r'[aeiou]', '', base_name))
        
        return variations

    def find_implicit_relationships(self) -> Dict[str, List[Dict]]:
        """
        Encontra relacionamentos implícitos entre tabelas baseado em padrões de nomenclatura.
        """
        relationships = defaultdict(list)
        table_names = {self._normalize_table_name(name): name 
                      for name in self.metadata.tables.keys()}

        for source_table_name, table in self.metadata.tables.items():
            for column in table.columns:
                column_name = column.name.lower()
   
                if column.foreign_keys:
                    continue

                matches = {}

                for pattern_name, pattern in self.relation_patterns.items():
                    match = pattern.match(column_name)
                    if match:
                        matched_part = match.group(1)
                        matches[pattern_name] = matched_part

                if matches:
                    for pattern_name, matched_part in matches.items():
                        possible_table_variations = self._get_all_table_variations(matched_part)
        
                        for variation in possible_table_variations:
                            if variation in table_names:
                                target_table = table_names[variation]
                                if target_table == source_table_name:
                                    continue

                                confidence = self._calculate_relationship_confidence(
                                    source_table_name, 
                                    target_table, 
                                    column_name, 
                                    pattern_name
                                )
                                
                                relationships[source_table_name].append({
                                    'source_column': column_name,
                                    'target_table': target_table,
                                    'pattern_matched': pattern_name,
                                    'confidence': confidence,
                                    'suggested_relationship': f"{source_table_name}.{column_name} -> {target_table}.id"
                                })

        return dict(relationships)

    def _calculate_relationship_confidence(self, 
                                        source_table: str, 
                                        target_table: str, 
                                        column_name: str, 
                                        pattern_type: str) -> float:
        """
        Calcula um score de confiança para o relacionamento implícito encontrado.
        """
        confidence = 0.0

        # Base confidence por tipo de padrão
        pattern_confidence = {
            'id_pattern': 0.8,
            'fk_pattern': 0.9,
            'table_id_pattern': 0.85,
            'ref_pattern': 0.7,
            'cod_pattern': 0.6,
            'num_pattern': 0.5
        }

        confidence = pattern_confidence.get(pattern_type, 0.3)

        try:
    
            query = f"""
                SELECT COUNT(DISTINCT a.{column_name}) as match_count
                FROM {source_table} a
                LEFT JOIN {target_table} b ON a.{column_name} = b.id
                WHERE b.id IS NOT NULL
            """
            result = self.engine.execute(query).scalar()
            
            if result > 0:
                confidence += 0.2
            
        except Exception as e:
            self.logger.warning(f"Erro ao verificar valores correspondentes: {str(e)}")

        return min(1.0, confidence)

    def analyze_and_suggest_relationships(self) -> Dict[str, any]:
        """
        Analisa e sugere relacionamentos, incluindo estatísticas e recomendações.
        """
        implicit_relations = self.find_implicit_relationships()

        analysis = {
            'relationships': implicit_relations,
            'statistics': {
                'total_implicit_relationships': sum(len(rels) for rels in implicit_relations.values()),
                'tables_with_implicit_relations': len(implicit_relations),
                'high_confidence_relations': sum(
                    1 for rels in implicit_relations.values() 
                    for rel in rels if rel['confidence'] >= 0.8
                )
            },
            'recommendations': []
        }

        # for table, relations in implicit_relations.items():
        #     for relation in relations:
        #         if relation['confidence'] >= 0.8:
        #             analysis['recommendations'].append({
        #                 'type': 'high_confidence',
        #                 'message': f"Considere adicionar uma foreign key para {relation['suggested_relationship']}",
        #                 'confidence': relation['confidence']
        #             })
        #         elif relation['confidence'] >= 0.6:
        #             analysis['recommendations'].append({
        #                 'type': 'medium_confidence',
        #                 'message': f"Verifique possível relação: {relation['suggested_relationship']}",
        #                 'confidence': relation['confidence']
        #             })

        return analysis

if __name__ == "__main__":


    load_dotenv()
    database_url = os.environ.get("database_url")


    if not database_url:
        raise ValueError("DATABASE_URL não encontrada nas variáveis de ambiente")


    analyzer = DatabaseAnalyzer(url=database_url)
    analysis = analyzer.analyze_and_suggest_relationships()
    

    print("\n=== Análise de Relacionamentos Implícitos ===")
    print(f"\nEstatísticas:")
    for key, value in analysis['statistics'].items():
        print(f"  {key.replace('_', ' ').title()}: {value}")


    print("\nRelacionamentos encontrados:")
    for table, relations in analysis['relationships'].items():
        print(f"\nTabela: {table}")
        for relation in sorted(relations, key=lambda x: x['confidence'], reverse=True):
            print(f"  - {relation['suggested_relationship']}")
            print(f"    Confiança: {relation['confidence']:.2f}")
            print(f"    Padrão: {relation['pattern_matched']}")

