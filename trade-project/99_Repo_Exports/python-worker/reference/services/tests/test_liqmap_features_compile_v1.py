def test_compile():
    import services.orderflow.liqmap_features
    assert services.orderflow.liqmap_features is not None

if __name__ == "__main__":
    test_compile()
    print("test_liqmap_features_compile_v1.py OK")
