import numpy as np
import zarr
from concurrent.futures import ThreadPoolExecutor
import time
import h5py
import os

def write_chunk_to_hdf5(dset, dataset, start_idx, end_idx):
    dset[start_idx:end_idx] = dataset[start_idx:end_idx]

def write_chunk_to_zarr(dset, dataset, start_idx, end_idx):
    dset[start_idx:end_idx] = dataset[start_idx:end_idx]

def write_to_hdf5_parallel(dataset, filename, chunks, num_workers):
    with h5py.File(filename, 'w') as file:
        dset = file.create_dataset('data', shape=dataset.shape, dtype=dataset.dtype, chunks=chunks)
        chunk_size = dataset.shape[0] // num_workers
        futures = []
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            for i in range(num_workers):
                start_idx = i * chunk_size
                end_idx = (i + 1) * chunk_size if i < num_workers - 1 else dataset.shape[0]
                future = executor.submit(write_chunk_to_hdf5, dset, dataset, start_idx, end_idx)
                futures.append(future)
        for future in futures:
            future.result()

def write_to_zarr_parallel(dataset, filename, chunks, num_workers):
    zarr_array = zarr.create(dataset.shape, chunks=chunks, store=filename, compressor=None, order='C')
    chunk_size = dataset.shape[0] // num_workers
    futures = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        for i in range(num_workers):
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size if i < num_workers - 1 else dataset.shape[0]
            future = executor.submit(write_chunk_to_zarr, zarr_array, dataset, start_idx, end_idx)
            futures.append(future)
    for future in futures:
        future.result()

dataset = np.random.random([1000, 1000, 1000])
chunks = (25, 1000, 1000)
h5_name = '/local/tmp/sample_parallel.h5'
zarr_name = '/local/tmp/sample_parallel.zarr'

for num_workers in [1, 2, 4, 8, 16]:
    os.system('rm -rf {h5} {zarr}'.format(h5=h5_name, zarr=zarr_name))
    t = time.time()
    write_to_zarr_parallel(dataset, zarr_name, chunks, num_workers)
    print('zarr num_workers={}: {}s'.format(num_workers, time.time() - t))
    t = time.time()
    write_to_hdf5_parallel(dataset, h5_name, chunks, num_workers)
    print('h5 num_workers={}: {}s'.format(num_workers, time.time() - t))
    print('')
