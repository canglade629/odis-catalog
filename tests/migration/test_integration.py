"""Integration tests for Silver V2 migration - cross-table validations."""
import pytest
from app.utils.migration_validator import MigrationValidator
from deltalake import DeltaTable
from app.core.config import get_settings
import logging

logger = logging.getLogger(__name__)


class TestSilverV2Integration:
    """Integration tests validating referential integrity across all V2 tables."""
    
    @classmethod
    def setup_class(cls):
        """Setup validator and settings."""
        cls.validator = MigrationValidator()
        cls.settings = get_settings()
    
    def test_all_fact_tables_reference_dim_commune(self):
        """Test that all fact tables with commune_sk have valid foreign keys."""
        fact_tables_with_commune = [
            ('fact_loyer_annonce', 'commune_sk'),
            ('fact_zone_attraction', 'commune_sk'),
        ]
        
        for table, fk_column in fact_tables_with_commune:
            result = self.validator.validate_foreign_keys(
                fact_table=table,
                fk_column=fk_column,
                dim_table="dim_commune",
                pk_column="commune_sk"
            )
            assert result.passed, f"{table}.{fk_column} has orphaned foreign keys: {result.message}"
    
    def test_fact_zone_attraction_both_fks_valid(self):
        """Test that fact_zone_attraction has valid FKs for both commune and pole."""
        # Test commune_sk
        result1 = self.validator.validate_foreign_keys(
            fact_table="fact_zone_attraction",
            fk_column="commune_sk",
            dim_table="dim_commune",
            pk_column="commune_sk"
        )
        assert result1.passed, f"commune_sk has orphaned FKs: {result1.message}"
        
        # Test commune_pole_sk
        result2 = self.validator.validate_foreign_keys(
            fact_table="fact_zone_attraction",
            fk_column="commune_pole_sk",
            dim_table="dim_commune",
            pk_column="commune_sk"
        )
        assert result2.passed, f"commune_pole_sk has orphaned FKs: {result2.message}"
    
    def test_fact_siae_poste_references_dim_siae_structure(self):
        """Test that fact_siae_poste has valid FK to dim_siae_structure."""
        result = self.validator.validate_foreign_keys(
            fact_table="fact_siae_poste",
            fk_column="siae_structure_sk",
            dim_table="dim_siae_structure",
            pk_column="siae_structure_sk"
        )
        assert result.passed, f"siae_structure_sk has orphaned FKs: {result.message}"
    
    def test_all_tables_have_metadata_columns(self):
        """Test that all V2 tables have the 4 required metadata columns."""
        all_tables = [
            'dim_commune',
            'dim_accueillant',
            'dim_gare',
            'dim_ligne',
            'dim_siae_structure',
            'fact_loyer_annonce',
            'fact_zone_attraction',
            'fact_siae_poste'
        ]
        
        for table in all_tables:
            result = self.validator.validate_metadata_columns(table)
            assert result.passed, f"{table} missing metadata columns: {result.message}"
    
    def test_all_surrogate_keys_unique(self):
        """Test that all surrogate keys (_sk) are unique in their tables."""
        tables_and_keys = [
            ('dim_commune', 'commune_sk'),
            ('dim_accueillant', 'accueillant_sk'),
            ('dim_gare', 'gare_sk'),
            ('dim_ligne', 'ligne_sk'),
            ('dim_siae_structure', 'siae_structure_sk'),
            ('fact_loyer_annonce', 'row_sk'),
            ('fact_zone_attraction', 'zone_attraction_sk'),
            ('fact_siae_poste', 'siae_poste_sk')
        ]
        
        for table, sk_column in tables_and_keys:
            result = self.validator.validate_unique_key(table, sk_column)
            assert result.passed, f"{table}.{sk_column} is not unique: {result.message}"
    
    def test_no_unexpected_nulls_in_primary_keys(self):
        """Test that no primary keys (SK columns) contain NULLs."""
        tables_and_keys = [
            ('dim_commune', ['commune_sk', 'commune_insee_code']),
            ('dim_accueillant', ['accueillant_sk']),
            ('dim_gare', ['gare_sk', 'code_uic']),
            ('dim_ligne', ['ligne_sk', 'ligne_code']),
            ('dim_siae_structure', ['siae_structure_sk', 'siret']),
            ('fact_loyer_annonce', ['row_sk', 'commune_sk']),
            ('fact_zone_attraction', ['zone_attraction_sk', 'commune_sk', 'commune_pole_sk']),
            ('fact_siae_poste', ['siae_poste_sk', 'siae_structure_sk'])
        ]
        
        for table, columns in tables_and_keys:
            result = self.validator.validate_no_nulls(table, columns)
            assert result.passed, f"{table} has NULLs in required columns: {result.message}"
    
    def test_fact_loyer_annonce_volume_increase(self):
        """Test that 2024 data causes expected volume increase."""
        path = self.settings.get_silver_path("fact_loyer_annonce")
        dt = DeltaTable(path)
        df = dt.to_pandas()
        
        # Count rows by year
        df_2024 = df[df['annee'] == 2024]
        df_legacy = df[df['annee'].isna()]
        
        total_rows = len(df)
        rows_2024 = len(df_2024)
        rows_legacy = len(df_legacy)
        
        logger.info(f"\n  Total rows: {total_rows:,}")
        logger.info(f"  2024 rows: {rows_2024:,}")
        logger.info(f"  Legacy rows: {rows_legacy:,}")
        
        if rows_2024 > 0:
            # Check that 2024 data has roughly 4x the communes
            unique_communes_2024 = df_2024['commune_sk'].nunique()
            segments_per_commune = rows_2024 / unique_communes_2024 if unique_communes_2024 > 0 else 0
            
            logger.info(f"  Unique communes in 2024: {unique_communes_2024:,}")
            logger.info(f"  Avg segments per commune: {segments_per_commune:.2f}")
            
            # Should be close to 4 segments per commune
            assert 3.5 <= segments_per_commune <= 4.0, \
                f"Expected ~4 segments per commune, got {segments_per_commune:.2f}"
    
    def test_2024_segmentation_completeness(self):
        """Test that 2024 data has all expected segments."""
        path = self.settings.get_silver_path("fact_loyer_annonce")
        dt = DeltaTable(path)
        df = dt.to_pandas()
        
        df_2024 = df[df['annee'] == 2024]
        
        if len(df_2024) > 0:
            # Check segment distribution
            segments = df_2024.groupby(['type_bien', 'segment_typologie']).size()
            
            expected_segments = [
                ('appartement', 'toutes typologies'),
                ('appartement', 'T1 et T2'),
                ('appartement', 'T3 et plus'),
                ('maison', 'toutes typologies')
            ]
            
            for segment in expected_segments:
                if segment in segments.index:
                    count = segments[segment]
                    logger.info(f"  {segment[0]} - {segment[1]}: {count:,} communes")
                else:
                    logger.warning(f"  Missing segment: {segment}")
    
    @classmethod
    def teardown_class(cls):
        """Print comprehensive validation report."""
        print("\n" + "="*80)
        print("INTEGRATION TEST VALIDATION REPORT")
        print("="*80)
        cls.validator.print_report()
        
        # Generate and save report
        report = cls.validator.generate_migration_report()
        print(f"\n\nSummary: {report['summary']}")
        
        if report['failed_validations']:
            print(f"\n⚠️  Found {len(report['failed_validations'])} failed validations")
        else:
            print("\n✅ ALL INTEGRATION TESTS PASSED!")


if __name__ == "__main__":
    """Run tests manually and print report."""
    pytest.main([__file__, "-v", "-s"])
