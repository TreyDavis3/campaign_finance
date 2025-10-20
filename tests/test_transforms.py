import pandas as pd
from src.etl import _normalize_str, _sha256_hex, transform_contributions_to_df


def test_normalize_and_hash():
    assert _normalize_str('  JOHN  DOE ') == 'john doe'
    assert _normalize_str(None) == ''
    h1 = _sha256_hex('a|b|c')
    h2 = _sha256_hex('a|b|c')
    assert h1 == h2
    assert len(h1) == 64


def test_transform_contributions_to_df():
    sample = {
        'results': [
            {
                'committee_id': 'C123',
                'contributor_name': 'Jane Doe',
                'contributor_city': 'Somewhere',
                'contributor_state': 'CA',
                'contributor_zip': '90210',
                'contribution_receipt_date': '2024-01-01',
                'contribution_receipt_amount': 250.0,
                'contributor_occupation': 'Engineer',
                'contributor_employer': 'ACME'
            }
        ]
    }
    df = transform_contributions_to_df(sample)
    assert isinstance(df, pd.DataFrame)
    assert df.iloc[0]['committee_id'] == 'C123'
    assert df.iloc[0]['contribution_amount'] == 250.0
