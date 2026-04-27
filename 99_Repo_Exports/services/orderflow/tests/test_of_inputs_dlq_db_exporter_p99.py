import unittest


class TestOFInputsDLQDBExporterP99(unittest.TestCase):
    def test_imports(self):
        # smoke-imports; avoids runtime deps like Redis
        import orderflow_services.of_inputs_dlq_db_exporter_p99 as exp
        import orderflow_services.of_inputs_dlq_db_drilldown_p99 as dd

        self.assertTrue(hasattr(exp, "Exporter"))
        self.assertTrue(callable(dd.main))

    def test_allowlist_default_nonempty(self):
        import orderflow_services.of_inputs_dlq_db_exporter_p99 as exp

        al = exp.DEFAULT_ALLOWLIST
        self.assertTrue(isinstance(al, list) and len(al) > 0)
        self.assertIn("missing_lob_fields", al)


if __name__ == "__main__":
    unittest.main()
