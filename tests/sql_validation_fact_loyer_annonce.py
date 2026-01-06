"""
SQL Syntax Validation for fact_loyer_annonce Pipeline.

This module validates the SQL query syntax without executing against actual data.
Uses DuckDB to parse and validate the query structure.
"""
import sys
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_sql_syntax_validation():
    """Test SQL query syntax using DuckDB parser."""
    try:
        import duckdb
        from app.pipelines.silver.fact_loyer_annonce import FactLoyerAnnoncePipeline
        
        logger.info("="*80)
        logger.info("SQL SYNTAX VALIDATION - fact_loyer_annonce")
        logger.info("="*80)
        
        # Get the pipeline instance
        pipeline = FactLoyerAnnoncePipeline()
        sql_query = pipeline.get_sql_query()
        
        logger.info(f"\n✓ Successfully retrieved SQL query ({len(sql_query)} characters)")
        
        # Create in-memory DuckDB connection
        conn = duckdb.connect(':memory:')
        
        # Create mock tables for syntax validation
        logger.info("\n→ Creating mock tables for validation...")
        
        # Mock bronze_logement table
        conn.execute("""
            CREATE TABLE bronze_logement (
                INSEE_C VARCHAR,
                loypredm2 VARCHAR,
                "lwr.IPm2" VARCHAR,
                "upr.IPm2" VARCHAR,
                TYPPRED VARCHAR,
                R2adj VARCHAR,
                R2_adj VARCHAR,
                NBobs_maille VARCHAR,
                nbobs_mail VARCHAR,
                NBobs_commune VARCHAR,
                nbobs_com VARCHAR,
                annee INTEGER,
                type_bien VARCHAR,
                segment_typologie VARCHAR,
                surface_ref DOUBLE,
                surface_piece_moy DOUBLE,
                ingestion_timestamp TIMESTAMP
            )
        """)
        
        # Mock silver_dim_commune table
        conn.execute("""
            CREATE TABLE silver_dim_commune (
                commune_sk VARCHAR,
                commune_insee_code VARCHAR,
                commune_label VARCHAR
            )
        """)
        
        logger.info("✓ Mock tables created successfully")
        
        # Try to parse the query
        logger.info("\n→ Validating SQL syntax...")
        try:
            # Use EXPLAIN to validate without executing
            explain_query = f"EXPLAIN {sql_query}"
            result = conn.execute(explain_query)
            logger.info("✓ SQL syntax is VALID")
            
            # Check query structure
            logger.info("\n→ Analyzing query structure...")
            
            # Count CTEs
            cte_count = sql_query.upper().count("WITH")
            logger.info(f"  • CTEs used: {cte_count}")
            
            # Check for SELECT
            if "SELECT" in sql_query.upper():
                logger.info("  • ✓ Contains SELECT statement")
            
            # Check for JOIN
            if "JOIN" in sql_query.upper():
                logger.info("  • ✓ Contains JOIN operations")
            
            # Check for WHERE clause
            if "WHERE" in sql_query.upper():
                logger.info("  • ✓ Contains WHERE clause")
            
            # Check for window functions
            if "ROW_NUMBER() OVER" in sql_query.upper():
                logger.info("  • ✓ Uses window functions (ROW_NUMBER)")
            
            # Check for CASE statements
            case_count = sql_query.upper().count("CASE")
            if case_count > 0:
                logger.info(f"  • ✓ Uses CASE statements ({case_count} found)")
            
            # Check for MD5 hash (surrogate key generation)
            if "MD5(" in sql_query.upper():
                logger.info("  • ✓ Uses MD5 for surrogate key generation")
            
            # Check for COALESCE (NULL handling)
            coalesce_count = sql_query.upper().count("COALESCE")
            if coalesce_count > 0:
                logger.info(f"  • ✓ Uses COALESCE for NULL handling ({coalesce_count} times)")
            
            return True
            
        except duckdb.ParserException as e:
            logger.error(f"✗ SQL SYNTAX ERROR: {e}")
            return False
        except Exception as e:
            logger.error(f"✗ VALIDATION ERROR: {e}")
            return False
        
        finally:
            conn.close()
            
    except ImportError as e:
        logger.error(f"✗ IMPORT ERROR: {e}")
        logger.error("  Make sure duckdb is installed: pip install duckdb")
        return False
    except Exception as e:
        logger.error(f"✗ UNEXPECTED ERROR: {e}")
        return False


def test_column_references():
    """Validate that all column references in the query are consistent."""
    try:
        from app.pipelines.silver.fact_loyer_annonce import FactLoyerAnnoncePipeline
        
        logger.info("\n→ Validating column references...")
        
        pipeline = FactLoyerAnnoncePipeline()
        sql_query = pipeline.get_sql_query()
        
        # Check for critical columns
        required_columns = {
            'INSEE_C': 'Input commune code',
            'loypredm2': 'Predicted rent',
            'lwr.IPm2': 'Lower bound',
            'upr.IPm2': 'Upper bound',
            'TYPPRED': 'Prediction type',
            'commune_sk': 'Commune surrogate key',
            'row_sk': 'Fact surrogate key'
        }
        
        all_found = True
        for col, description in required_columns.items():
            if col in sql_query or col.replace('.', '') in sql_query:
                logger.info(f"  • ✓ {description} ({col})")
            else:
                logger.error(f"  • ✗ MISSING: {description} ({col})")
                all_found = False
        
        # Check new 2024 columns
        new_columns = ['annee', 'type_bien', 'segment_typologie', 'surface_ref', 'surface_piece_moy']
        logger.info("\n  New 2024 columns:")
        for col in new_columns:
            if col in sql_query:
                logger.info(f"  • ✓ {col}")
            else:
                logger.warning(f"  • ✗ {col} not found")
        
        return all_found
        
    except Exception as e:
        logger.error(f"✗ COLUMN VALIDATION ERROR: {e}")
        return False


def test_data_quality_checks():
    """Validate that data quality checks are present in the query."""
    try:
        from app.pipelines.silver.fact_loyer_annonce import FactLoyerAnnoncePipeline
        
        logger.info("\n→ Validating data quality checks...")
        
        pipeline = FactLoyerAnnoncePipeline()
        sql_query = pipeline.get_sql_query()
        
        quality_checks = {
            'IS NOT NULL': 'NULL checking',
            '> 0': 'Positive value validation',
            'lwr_clean < l.upr_clean': 'Bounds coherence check',
            'ROW_NUMBER()': 'Deduplication logic',
            'PARTITION BY': 'Partitioning for deduplication'
        }
        
        all_found = True
        for check, description in quality_checks.items():
            if check in sql_query:
                logger.info(f"  • ✓ {description}")
            else:
                logger.warning(f"  • ⚠ {description} not explicitly found")
                all_found = False
        
        return all_found
        
    except Exception as e:
        logger.error(f"✗ QUALITY CHECKS VALIDATION ERROR: {e}")
        return False


def main():
    """Run all SQL validation tests."""
    logger.info("\n" + "="*80)
    logger.info("FACT_LOYER_ANNONCE - SQL VALIDATION TEST SUITE")
    logger.info("="*80 + "\n")
    
    results = {
        'Syntax Validation': test_sql_syntax_validation(),
        'Column References': test_column_references(),
        'Quality Checks': test_data_quality_checks()
    }
    
    # Summary
    logger.info("\n" + "="*80)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*80)
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{status:10} | {test_name}")
    
    logger.info("="*80)
    logger.info(f"TOTAL: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("✓ SQL VALIDATION: ALL TESTS PASSED")
        logger.info("="*80)
        return 0
    else:
        logger.error("✗ SQL VALIDATION: SOME TESTS FAILED")
        logger.info("="*80)
        return 1


if __name__ == "__main__":
    sys.exit(main())

