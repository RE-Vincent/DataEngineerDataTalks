if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    data = data.dropna(subset=['VendorID'])
    data = data.query("trip_distance!=0 and passenger_count!=0").reset_index(drop=True)
    data = data.rename(columns={'VendorID':'vendor_id',
                            'RatecodeID':'ratecode_id',
                            'PULocationID':'pu_location_id',
                            'DOLocationID':'do_location_id'})
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, "Passenger_count == 0 shoul be 0"
    assert output['trip_distance'].isin([0]).sum() == 0, "trip_distance == 0 shoul be 0"
    assert output['vendor_id'].isnull().sum() == 0, "vendor_id it must not be null"
