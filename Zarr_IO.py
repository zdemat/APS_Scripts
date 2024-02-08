import zarr
import numpy as np
import dxchange
import matplotlib.pyplot as plt
import os

# Open the existing Zarr file
def readAPSZarr(file_name, proj=None, sino=None):
	root = zarr.open(file_name, mode='r')

	attributes_dict = {}
	for key, value in root.attrs.items():
	    attributes_dict[key] = value
	    
	[start_index[0]:end_index[0], start_index[1]:end_index[1], start_index[2]:end_index[2]]

	return root['exchange/data'],root['exchange/data_white'], root['exchange/data_dark'], root['exchange/theta'], attributes_dict


def h5tozarrAPS(file_name, proj=None, sino=None):
	meta = dxchange.reader.read_hdf_meta(file_name)
	proj, flat, dark, theta = dxchange.exchange.read_aps_tomoscan_hdf5(file_name, proj=None, sino=None)

	# Extract the file name and extension from the path
	base_name, _ = os.path.splitext(os.path.basename(file_name))
	zarr_name = base_name + '.zarr'
	zarr_path = os.path.join(os.path.dirname(file_name), zarr_name)

	# Create a structured Zarr file
	root = zarr.open(zarr_path, mode='w')

	# Create data for /test/data1 if it doesn't exist
	group_test = root.require_group('exchange')
	if 'data' not in group_test:
	    #data = np.random.rand(100, 100, 100)
	    group_test.create_dataset('data', data=proj)

	# Create data for /test/data2 if it doesn't exist
	if 'data_flat' not in group_test:
	    #data_flat = np.random.rand(100, 100, 10)
	    group_test.create_dataset('data_flat', data=flat)
	    
	if 'data_dark' not in group_test:
	    #data_dark = np.random.rand(100, 100, 10)
	    group_test.create_dataset('data_dark', data=dark)
	    
	if 'theta' not in group_test:
	    #theta = np.random.rand(100)
	    group_test.create_dataset('theta', data=theta)

	group_D = root.require_group('default')
	group_I = root.require_group('instrument')
	group_P = root.require_group('process')


	for key, value in meta.items():
	    root.attrs[key] = value
