"""
Unit tests for Bronze Logement Pipeline.

Tests the filename classification and column enrichment logic for 2024 data migration.
"""
import pytest
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.pipelines.bronze.logement import BronzeLogementPipeline


class TestBronzeLogementFilenameClassification:
    """Test suite for filename-based housing type classification."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.pipeline = BronzeLogementPipeline()
    
    def test_identify_appartement_toutes_typologies(self):
        """Test identification of appartement - toutes typologies file."""
        result = self.pipeline._identify_file_type('pred-app-mef-dhup.csv')
        
        assert result['type_bien'] == 'appartement'
        assert result['segment_typologie'] == 'toutes typologies'
        assert result['surface_ref'] == 52.0
        assert result['surface_piece_moy'] == 22.2
    
    def test_identify_appartement_t1_t2(self):
        """Test identification of appartement - T1 et T2 file."""
        result = self.pipeline._identify_file_type('pred-app12-mef-dhup.csv')
        
        assert result['type_bien'] == 'appartement'
        assert result['segment_typologie'] == 'T1 et T2'
        assert result['surface_ref'] == 37.0
        assert result['surface_piece_moy'] == 23.0
    
    def test_identify_appartement_t3_plus(self):
        """Test identification of appartement - T3 et plus file."""
        result = self.pipeline._identify_file_type('pred-app3-mef-dhup.csv')
        
        assert result['type_bien'] == 'appartement'
        assert result['segment_typologie'] == 'T3 et plus'
        assert result['surface_ref'] == 72.0
        assert result['surface_piece_moy'] == 21.2
    
    def test_identify_maison_toutes_typologies(self):
        """Test identification of maison - toutes typologies file."""
        result = self.pipeline._identify_file_type('pred-mai-mef-dhup.csv')
        
        assert result['type_bien'] == 'maison'
        assert result['segment_typologie'] == 'toutes typologies'
        assert result['surface_ref'] == 92.0
        assert result['surface_piece_moy'] == 22.4
    
    def test_identify_uppercase_filename(self):
        """Test that uppercase filenames are handled correctly."""
        result = self.pipeline._identify_file_type('PRED-APP-MEF-DHUP.CSV')
        
        assert result['type_bien'] == 'appartement'
        assert result['segment_typologie'] == 'toutes typologies'
    
    def test_identify_unknown_file_returns_default(self):
        """Test that unknown filenames return default values."""
        result = self.pipeline._identify_file_type('unknown-file.csv')
        
        # Should return default appartement, toutes typologies
        assert result['type_bien'] == 'appartement'
        assert result['segment_typologie'] == 'toutes typologies'
        assert result['surface_ref'] == 52.0


class TestBronzeLogementTransform:
    """Test suite for data transformation logic."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.pipeline = BronzeLogementPipeline()
    
    def test_transform_2024_file_adds_columns(self):
        """Test that 2024 files get new columns added."""
        # Create sample DataFrame
        df = pd.DataFrame({
            'INSEE_C': ['01001', '01002'],
            'loypredm2': ['10.5', '12.3']
        })
        
        # Transform with 2024 filename
        result = self.pipeline.transform(df, 'gs://bucket/raw/logement/pred-app-mef-dhup.csv')
        
        # Check new columns were added
        assert 'annee' in result.columns
        assert 'type_bien' in result.columns
        assert 'segment_typologie' in result.columns
        assert 'surface_ref' in result.columns
        assert 'surface_piece_moy' in result.columns
        assert 'ingestion_timestamp' in result.columns
        
        # Check values
        assert result['annee'].iloc[0] == 2024
        assert result['type_bien'].iloc[0] == 'appartement'
        assert result['segment_typologie'].iloc[0] == 'toutes typologies'
        assert result['surface_ref'].iloc[0] == 52.0
    
    def test_transform_legacy_file_adds_null_columns(self):
        """Test that legacy files get NULL columns for compatibility."""
        df = pd.DataFrame({
            'INSEE_C': ['01001', '01002'],
            'loypredm2': ['10.5', '12.3']
        })
        
        # Transform with legacy filename (doesn't start with 'pred-')
        result = self.pipeline.transform(df, 'gs://bucket/raw/logement/loyers_2018.csv')
        
        # Check new columns were added as NULL
        assert 'annee' in result.columns
        assert 'type_bien' in result.columns
        assert pd.isna(result['annee'].iloc[0])
        assert pd.isna(result['type_bien'].iloc[0])
    
    def test_transform_extracts_timestamp_from_filename(self):
        """Test timestamp extraction from filename."""
        df = pd.DataFrame({
            'INSEE_C': ['01001'],
            'loypredm2': ['10.5']
        })
        
        # Transform with timestamped filename
        result = self.pipeline.transform(
            df, 
            'gs://bucket/raw/logement/pred-app-mef-dhup_20241205_143000.csv'
        )
        
        # Check timestamp was extracted
        assert 'ingestion_timestamp' in result.columns
        timestamp = result['ingestion_timestamp'].iloc[0]
        assert isinstance(timestamp, datetime)
        assert timestamp.year == 2024
        assert timestamp.month == 12
        assert timestamp.day == 5
    
    def test_transform_handles_missing_timestamp(self):
        """Test that files without timestamp in name get current timestamp."""
        df = pd.DataFrame({
            'INSEE_C': ['01001'],
            'loypredm2': ['10.5']
        })
        
        # Transform without timestamp in filename
        result = self.pipeline.transform(df, 'gs://bucket/raw/logement/pred-app-mef-dhup.csv')
        
        # Check timestamp was added (should be current time)
        assert 'ingestion_timestamp' in result.columns
        timestamp = result['ingestion_timestamp'].iloc[0]
        assert isinstance(timestamp, datetime)


class TestBronzeLogementMapping:
    """Test the filename mapping configuration."""
    
    def test_all_mappings_have_required_keys(self):
        """Test that all mappings have the required keys."""
        pipeline = BronzeLogementPipeline()
        required_keys = {'type_bien', 'segment_typologie', 'surface_ref', 'surface_piece_moy'}
        
        for pattern, mapping in pipeline.FILENAME_MAPPING.items():
            assert set(mapping.keys()) == required_keys, \
                f"Mapping for {pattern} missing required keys"
    
    def test_surface_values_are_positive(self):
        """Test that all surface values are positive."""
        pipeline = BronzeLogementPipeline()
        
        for pattern, mapping in pipeline.FILENAME_MAPPING.items():
            assert mapping['surface_ref'] > 0, \
                f"surface_ref for {pattern} must be positive"
            assert mapping['surface_piece_moy'] > 0, \
                f"surface_piece_moy for {pattern} must be positive"
    
    def test_type_bien_values_are_valid(self):
        """Test that type_bien values are valid."""
        pipeline = BronzeLogementPipeline()
        valid_types = {'appartement', 'maison'}
        
        for pattern, mapping in pipeline.FILENAME_MAPPING.items():
            assert mapping['type_bien'] in valid_types, \
                f"type_bien for {pattern} must be 'appartement' or 'maison'"
    
    def test_segment_typologie_values_are_valid(self):
        """Test that segment_typologie values are valid."""
        pipeline = BronzeLogementPipeline()
        valid_segments = {'toutes typologies', 'T1 et T2', 'T3 et plus'}
        
        for pattern, mapping in pipeline.FILENAME_MAPPING.items():
            assert mapping['segment_typologie'] in valid_segments, \
                f"segment_typologie for {pattern} must be one of {valid_segments}"


def test_pipeline_registration():
    """Test that the pipeline is properly registered."""
    from app.core.pipeline_registry import get_pipeline
    
    # Should be able to get the pipeline by name
    pipeline = get_pipeline('bronze', 'logement')
    assert pipeline is not None
    assert isinstance(pipeline, BronzeLogementPipeline)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

