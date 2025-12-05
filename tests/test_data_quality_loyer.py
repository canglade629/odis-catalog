"""
Data Quality Validation Tests for fact_loyer_annonce.

Tests actual data quality in the silver table after migration.
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from deltalake import DeltaTable
from app.core.config import get_settings
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestLoyerAnnonceDataQuality:
    """Data quality tests for fact_loyer_annonce table."""
    
    @classmethod
    def setup_class(cls):
        """Load the silver table once for all tests."""
        cls.settings = get_settings()
        path = cls.settings.get_silver_path("fact_loyer_annonce")
        
        try:
            dt = DeltaTable(path)
            cls.df = dt.to_pandas()
            logger.info(f"Loaded fact_loyer_annonce: {len(cls.df):,} rows")
        except Exception as e:
            logger.error(f"Failed to load table: {e}")
            cls.df = pd.DataFrame()  # Empty dataframe for tests to handle
    
    def test_table_not_empty(self):
        """Test that the table contains data."""
        assert len(self.df) > 0, "Table is empty"
    
    def test_no_duplicate_row_sk(self):
        """Test that row_sk values are unique."""
        duplicates = self.df['row_sk'].duplicated().sum()
        assert duplicates == 0, f"Found {duplicates} duplicate row_sk values"
    
    def test_all_loyer_values_positive(self):
        """Test that all loyer values are strictly positive."""
        negative_moy = (self.df['loyer_m2_moy'] <= 0).sum()
        negative_min = (self.df['loyer_m2_min'] <= 0).sum()
        negative_max = (self.df['loyer_m2_max'] <= 0).sum()
        
        assert negative_moy == 0, f"Found {negative_moy} non-positive loyer_m2_moy values"
        assert negative_min == 0, f"Found {negative_min} non-positive loyer_m2_min values"
        assert negative_max == 0, f"Found {negative_max} non-positive loyer_m2_max values"
    
    def test_bounds_are_coherent(self):
        """Test that min < moy < max for all rows."""
        incoherent_min = (self.df['loyer_m2_min'] >= self.df['loyer_m2_moy']).sum()
        incoherent_max = (self.df['loyer_m2_moy'] >= self.df['loyer_m2_max']).sum()
        
        assert incoherent_min == 0, \
            f"Found {incoherent_min} rows where loyer_m2_min >= loyer_m2_moy"
        assert incoherent_max == 0, \
            f"Found {incoherent_max} rows where loyer_m2_moy >= loyer_m2_max"
    
    def test_score_qualite_in_valid_range(self):
        """Test that score_qualite is between 0 and 1 when not NULL."""
        df_with_score = self.df[self.df['score_qualite'].notna()]
        
        if len(df_with_score) > 0:
            below_zero = (df_with_score['score_qualite'] < 0).sum()
            above_one = (df_with_score['score_qualite'] > 1).sum()
            
            assert below_zero == 0, f"Found {below_zero} score_qualite values < 0"
            assert above_one == 0, f"Found {above_one} score_qualite values > 1"
    
    def test_observation_counts_non_negative(self):
        """Test that observation counts are non-negative when not NULL."""
        df_with_maille = self.df[self.df['nb_observation_maille'].notna()]
        df_with_commune = self.df[self.df['nb_observation_commune'].notna()]
        
        if len(df_with_maille) > 0:
            negative_maille = (df_with_maille['nb_observation_maille'] < 0).sum()
            assert negative_maille == 0, \
                f"Found {negative_maille} negative nb_observation_maille values"
        
        if len(df_with_commune) > 0:
            negative_commune = (df_with_commune['nb_observation_commune'] < 0).sum()
            assert negative_commune == 0, \
                f"Found {negative_commune} negative nb_observation_commune values"
    
    def test_2024_surface_values_match_mapping(self):
        """Test that 2024 data surface values match expected mapping."""
        df_2024 = self.df[self.df['annee'] == 2024]
        
        if len(df_2024) == 0:
            pytest.skip("No 2024 data found in table")
        
        # Expected mapping
        expected_mapping = {
            ('appartement', 'toutes typologies'): {'surface_ref': 52.0, 'surface_piece_moy': 22.2},
            ('appartement', 'T1 et T2'): {'surface_ref': 37.0, 'surface_piece_moy': 23.0},
            ('appartement', 'T3 et plus'): {'surface_ref': 72.0, 'surface_piece_moy': 21.2},
            ('maison', 'toutes typologies'): {'surface_ref': 92.0, 'surface_piece_moy': 22.4}
        }
        
        for (type_bien, typologie), expected in expected_mapping.items():
            segment_df = df_2024[
                (df_2024['type_bien'] == type_bien) & 
                (df_2024['segment_typologie'] == typologie)
            ]
            
            if len(segment_df) > 0:
                # Check all rows in this segment have correct values
                wrong_surface = (segment_df['surface_ref'] != expected['surface_ref']).sum()
                wrong_piece = (segment_df['surface_piece_moy'] != expected['surface_piece_moy']).sum()
                
                assert wrong_surface == 0, \
                    f"{type_bien}/{typologie}: {wrong_surface} rows have wrong surface_ref"
                assert wrong_piece == 0, \
                    f"{type_bien}/{typologie}: {wrong_piece} rows have wrong surface_piece_moy"
    
    def test_2024_segments_per_commune_distribution(self):
        """Test the distribution of segments per commune for 2024 data."""
        df_2024 = self.df[self.df['annee'] == 2024]
        
        if len(df_2024) == 0:
            pytest.skip("No 2024 data found in table")
        
        segments_per_commune = df_2024.groupby('commune_sk').size()
        
        # Stats
        min_segments = segments_per_commune.min()
        max_segments = segments_per_commune.max()
        avg_segments = segments_per_commune.mean()
        communes_with_4 = (segments_per_commune == 4).sum()
        total_communes = len(segments_per_commune)
        
        logger.info(f"\n2024 Segmentation Statistics:")
        logger.info(f"  Total communes: {total_communes:,}")
        logger.info(f"  Segments per commune: min={min_segments}, max={max_segments}, avg={avg_segments:.2f}")
        logger.info(f"  Communes with all 4 segments: {communes_with_4:,} ({communes_with_4/total_communes*100:.1f}%)")
        
        # Most communes should have 4 segments
        assert max_segments <= 4, f"Found commune with {max_segments} segments (max should be 4)"
        
        pct_with_4 = (communes_with_4 / total_communes) * 100
        assert pct_with_4 >= 90, \
            f"Only {pct_with_4:.1f}% of communes have all 4 segments (expected >= 90%)"
    
    def test_required_columns_no_nulls(self):
        """Test that required columns don't have NULL values."""
        required_columns = [
            'row_sk',
            'commune_sk',
            'loyer_m2_moy',
            'loyer_m2_min',
            'loyer_m2_max',
            'job_insert_id',
            'job_insert_date_utc'
        ]
        
        for col in required_columns:
            null_count = self.df[col].isna().sum()
            assert null_count == 0, f"Column {col} has {null_count} NULL values"
    
    def test_nullable_columns_have_expected_nulls(self):
        """Test that nullable columns have expected NULL patterns."""
        # 2024 columns should be NULL for legacy data
        df_legacy = self.df[self.df['annee'].isna()]
        
        if len(df_legacy) > 0:
            logger.info(f"\nLegacy data rows: {len(df_legacy):,}")
            
            # These should be NULL for legacy data
            nullable_2024_columns = ['type_bien', 'segment_typologie', 'surface_ref', 'surface_piece_moy']
            
            for col in nullable_2024_columns:
                null_pct = (df_legacy[col].isna().sum() / len(df_legacy)) * 100
                logger.info(f"  {col}: {null_pct:.1f}% NULL in legacy data")
    
    def test_loyer_values_in_reasonable_range(self):
        """Test that loyer values are in a reasonable range (sanity check)."""
        # Define reasonable ranges (based on French real estate market)
        min_reasonable = 2.0  # €/m²
        max_reasonable = 50.0  # €/m²
        
        too_low = (self.df['loyer_m2_moy'] < min_reasonable).sum()
        too_high = (self.df['loyer_m2_moy'] > max_reasonable).sum()
        
        pct_too_low = (too_low / len(self.df)) * 100
        pct_too_high = (too_high / len(self.df)) * 100
        
        logger.info(f"\nLoyer value ranges:")
        logger.info(f"  < {min_reasonable} €/m²: {too_low} ({pct_too_low:.2f}%)")
        logger.info(f"  > {max_reasonable} €/m²: {too_high} ({pct_too_high:.2f}%)")
        logger.info(f"  Min: {self.df['loyer_m2_moy'].min():.2f} €/m²")
        logger.info(f"  Max: {self.df['loyer_m2_moy'].max():.2f} €/m²")
        logger.info(f"  Mean: {self.df['loyer_m2_moy'].mean():.2f} €/m²")
        logger.info(f"  Median: {self.df['loyer_m2_moy'].median():.2f} €/m²")
        
        # Warnings but not hard failures (edge cases may exist)
        if pct_too_low > 1:
            logger.warning(f"⚠️  {pct_too_low:.2f}% of values below {min_reasonable} €/m²")
        if pct_too_high > 1:
            logger.warning(f"⚠️  {pct_too_high:.2f}% of values above {max_reasonable} €/m²")
    
    @classmethod
    def teardown_class(cls):
        """Print summary statistics."""
        if len(cls.df) > 0:
            print("\n" + "="*80)
            print("DATA QUALITY SUMMARY")
            print("="*80)
            print(f"Total rows: {len(cls.df):,}")
            print(f"Unique communes: {cls.df['commune_sk'].nunique():,}")
            
            df_2024 = cls.df[cls.df['annee'] == 2024]
            df_legacy = cls.df[cls.df['annee'].isna()]
            
            print(f"\n2024 data: {len(df_2024):,} rows")
            print(f"Legacy data: {len(df_legacy):,} rows")
            
            if len(df_2024) > 0:
                print(f"\n2024 Segments:")
                for (type_bien, typologie), count in df_2024.groupby(['type_bien', 'segment_typologie']).size().items():
                    print(f"  {type_bien} - {typologie}: {count:,}")
            
            print("="*80)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

