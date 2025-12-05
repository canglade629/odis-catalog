#!/usr/bin/env python3
"""
Master Test Execution Script for fact_loyer_annonce Pipeline.

Orchestrates all tests and generates a comprehensive report.
"""
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
TESTS_DIR = PROJECT_ROOT / "tests"


class TestRunner:
    """Orchestrates test execution and reporting."""
    
    def __init__(self):
        self.results = []
        self.start_time = datetime.now()
    
    def run_test_file(self, test_file: Path, description: str) -> dict:
        """Run a single test file and capture results."""
        logger.info(f"\n{'='*80}")
        logger.info(f"Running: {description}")
        logger.info(f"File: {test_file}")
        logger.info(f"{'='*80}\n")
        
        start = datetime.now()
        
        try:
            # Run pytest on the file
            result = subprocess.run(
                [sys.executable, "-m", "pytest", str(test_file), "-v", "-s"],
                capture_output=True,
                text=True,
                cwd=PROJECT_ROOT
            )
            
            duration = (datetime.now() - start).total_seconds()
            
            # Parse output
            passed = result.returncode == 0
            output = result.stdout + result.stderr
            
            # Count test results
            passed_count = output.count(" PASSED")
            failed_count = output.count(" FAILED")
            skipped_count = output.count(" SKIPPED")
            
            test_result = {
                'description': description,
                'file': str(test_file.name),
                'passed': passed,
                'duration': duration,
                'passed_tests': passed_count,
                'failed_tests': failed_count,
                'skipped_tests': skipped_count,
                'output': output
            }
            
            self.results.append(test_result)
            
            if passed:
                logger.info(f"✅ {description}: PASSED ({passed_count} tests, {duration:.2f}s)")
            else:
                logger.error(f"❌ {description}: FAILED ({failed_count} failures, {duration:.2f}s)")
            
            return test_result
            
        except Exception as e:
            logger.error(f"❌ {description}: ERROR - {e}")
            duration = (datetime.now() - start).total_seconds()
            
            test_result = {
                'description': description,
                'file': str(test_file.name),
                'passed': False,
                'duration': duration,
                'passed_tests': 0,
                'failed_tests': 0,
                'skipped_tests': 0,
                'error': str(e)
            }
            
            self.results.append(test_result)
            return test_result
    
    def run_all_tests(self):
        """Run all test suites in sequence."""
        logger.info("\n" + "="*80)
        logger.info("FACT_LOYER_ANNONCE - COMPREHENSIVE TEST SUITE")
        logger.info("="*80)
        logger.info(f"Start time: {self.start_time.isoformat()}")
        logger.info("="*80 + "\n")
        
        # Define test suite
        test_suite = [
            {
                'file': TESTS_DIR / "sql_validation_fact_loyer_annonce.py",
                'description': "SQL Syntax Validation"
            },
            {
                'file': TESTS_DIR / "test_bronze_logement.py",
                'description': "Bronze Layer Unit Tests"
            },
            {
                'file': TESTS_DIR / "migration" / "test_fact_loyer_annonce.py",
                'description': "Silver Layer Unit Tests"
            },
            {
                'file': TESTS_DIR / "migration" / "test_integration.py",
                'description': "Integration Tests"
            },
            {
                'file': TESTS_DIR / "test_data_quality_loyer.py",
                'description': "Data Quality Validation"
            }
        ]
        
        # Run each test file
        for test_info in test_suite:
            test_file = test_info['file']
            description = test_info['description']
            
            if test_file.exists():
                self.run_test_file(test_file, description)
            else:
                logger.warning(f"⚠️  Test file not found: {test_file}")
                self.results.append({
                    'description': description,
                    'file': str(test_file.name),
                    'passed': False,
                    'duration': 0,
                    'error': 'File not found'
                })
    
    def generate_report(self):
        """Generate comprehensive test report."""
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        # Calculate stats
        total_suites = len(self.results)
        passed_suites = sum(1 for r in self.results if r['passed'])
        failed_suites = total_suites - passed_suites
        
        total_tests = sum(r.get('passed_tests', 0) + r.get('failed_tests', 0) + r.get('skipped_tests', 0) 
                         for r in self.results)
        total_passed = sum(r.get('passed_tests', 0) for r in self.results)
        total_failed = sum(r.get('failed_tests', 0) for r in self.results)
        total_skipped = sum(r.get('skipped_tests', 0) for r in self.results)
        
        # Generate report
        report_lines = []
        report_lines.append("="*80)
        report_lines.append("TEST EXECUTION REPORT - fact_loyer_annonce Pipeline")
        report_lines.append("="*80)
        report_lines.append(f"Start Time: {self.start_time.isoformat()}")
        report_lines.append(f"End Time: {end_time.isoformat()}")
        report_lines.append(f"Total Duration: {total_duration:.2f}s")
        report_lines.append("")
        report_lines.append("="*80)
        report_lines.append("SUMMARY")
        report_lines.append("="*80)
        report_lines.append(f"Test Suites: {passed_suites}/{total_suites} passed")
        report_lines.append(f"Individual Tests: {total_passed}/{total_tests} passed")
        report_lines.append(f"  ✅ Passed: {total_passed}")
        report_lines.append(f"  ❌ Failed: {total_failed}")
        report_lines.append(f"  ⊘ Skipped: {total_skipped}")
        report_lines.append("")
        report_lines.append("="*80)
        report_lines.append("DETAILED RESULTS")
        report_lines.append("="*80)
        
        for i, result in enumerate(self.results, 1):
            status = "✅ PASS" if result['passed'] else "❌ FAIL"
            report_lines.append(f"\n{i}. {result['description']}")
            report_lines.append(f"   Status: {status}")
            report_lines.append(f"   File: {result['file']}")
            report_lines.append(f"   Duration: {result['duration']:.2f}s")
            
            if 'error' in result:
                report_lines.append(f"   Error: {result['error']}")
            else:
                report_lines.append(f"   Tests: {result.get('passed_tests', 0)} passed, "
                                  f"{result.get('failed_tests', 0)} failed, "
                                  f"{result.get('skipped_tests', 0)} skipped")
        
        report_lines.append("")
        report_lines.append("="*80)
        
        if failed_suites == 0:
            report_lines.append("✅ ALL TESTS PASSED")
            report_lines.append("="*80)
            report_lines.append("")
            report_lines.append("Pipeline is ready for deployment!")
        else:
            report_lines.append("❌ SOME TESTS FAILED")
            report_lines.append("="*80)
            report_lines.append("")
            report_lines.append("Please review failed tests above and fix issues before deployment.")
        
        report_lines.append("")
        
        # Print report
        report_text = "\n".join(report_lines)
        print(report_text)
        
        # Save report to file
        report_file = PROJECT_ROOT / "TEST_REPORT_fact_loyer_annonce.md"
        with open(report_file, 'w') as f:
            f.write("# Test Execution Report: fact_loyer_annonce Pipeline\n\n")
            f.write(f"**Generated:** {end_time.isoformat()}\n\n")
            f.write("```\n")
            f.write(report_text)
            f.write("\n```\n")
        
        logger.info(f"\n📄 Full report saved to: {report_file}")
        
        return failed_suites == 0


def main():
    """Main execution function."""
    runner = TestRunner()
    
    try:
        # Run all tests
        runner.run_all_tests()
        
        # Generate report
        success = runner.generate_report()
        
        # Return exit code
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  Test execution interrupted by user")
        return 130
    except Exception as e:
        logger.error(f"\n\n❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

