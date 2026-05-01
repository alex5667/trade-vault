
import html

def test_escaping():
    test_cases = [
        (""),
        ("total=0<min_total=5", "total=0&lt;min_total=5"),
        ("tail(R<=-1)", "tail(R&lt;=-1)"),
        ("bigwin(R>=2)", "bigwin(R&gt;=2)"),
        ("CryptoOrderFlow", "CryptoOrderFlow"),
        ("status: NO_DATA total=0<min_total=5", "status: NO_DATA total=0&lt;min_total=5"),
    ]
    
    for input_str, expected in test_cases:
        escaped = html.escape(input_str)
        print(f"Input: {input_str}")
        print(f"Escaped: {escaped}")
        assert escaped == expected or (input_str == "tail(R<=-1)" and escaped == "tail(R&lt;=-1)")
    
    print("All basic escaping tests passed!")

if __name__ == "__main__":
    test_escaping()
