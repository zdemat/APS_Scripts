import numpy as np
import zarr
from concurrent.futures import ThreadPoolExecutor
import time
import h5py
import os

# MPI install: conda create -n zarr-APS-24a -c conda-forge zarr h5py=*=*mpi_openmpi* hdf5=*=mpi_openmpi* blas=*=*mkl numpy ipykernel

try:
    from mpi4py import MPI
    # a common MPI communicator
    comm = MPI.COMM_WORLD
except:
    print('no mpi4py')
    MPI = None
    comm = None

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

# parallel-HDF5 auxulliary function
# there is a bug in libhdf5: very large datasets (>4GB) written by hdf5parallel lib must be FILL_TIME_NEVER
def _create_dataset_nofill(group, name, shape, dtype, chunks=None):
    spaceid = h5py.h5s.create_simple(shape)
    cplist = h5py.h5p.create(h5py.h5p.DATASET_CREATE)
    cplist.set_fill_time(h5py.h5d.FILL_TIME_NEVER)
    if chunks not in [None,[]] and isinstance(chunks, tuple):
        cplist.set_chunk(chunks)
    typeid = h5py.h5t.py_create(dtype)
    datasetid = h5py.h5d.create(group.file.id, (group.name+'/'+name).encode('utf-8'), typeid, spaceid, cplist)
    dset = h5py.Dataset(datasetid)
    return dset
    
def write_to_hdf5p_parallel(dataset, filename, chunks, ntasks=None):
    # common MPI comminicator (we could also send it as an argument)
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    ntasks = comm.Get_size() if ntasks is None else ntasks
    # save data to hdf5
    with h5py.File(filename, 'w', driver='mpio', comm=MPI.COMM_WORLD) as file:
        comm.barrier() # all tasks have file open
        dset = _create_dataset_nofill(file['/'], name='data', shape=dataset.shape, dtype=dataset.dtype, chunks=chunks)
        comm.barrier() # all tasks created dataset (and set all properties)

        nb_chunks = (dataset.shape[0] + chunks[0]-1) // chunks[0]
        chunks_per_task = (nb_chunks + ntasks-1) // ntasks
        rank_offset = rank * chunks[0]
        for ichunk in range(chunks_per_task):
            start_idx = rank_offset + ichunk * chunks[0] * ntasks
            end_idx = (start_idx + chunks[0]) if (start_idx + chunks[0]) < dataset.shape[0] else dataset.shape[0]
            dset[start_idx:end_idx] = dataset[start_idx:end_idx]

        comm.barrier() # all tasks done

dataset_shape = (1000,1000,1000)

# MPI: Let's assume the first task (rank=0) holds all data.
# We will use shared memory to acces these data by other writes tasks.
if MPI is not None:
    itemsize = MPI.DOUBLE.Get_size() 
    if comm.Get_rank() == 0:
        nbytes = np.prod(dataset_shape) * itemsize 
    else: 
        nbytes = 0
    
    # on rank 0, create the shared block
    # on other ranks get a handle to it (known as a window in MPI)
    win = MPI.Win.Allocate_shared(nbytes, itemsize, comm=comm) 

    # create a numpy array whose data points to the shared mem
    buf, itemsize = win.Shared_query(0) 
    assert itemsize == MPI.DOUBLE.Get_size() 
    dataset = np.ndarray(buffer=buf, dtype=np.float64, shape=dataset_shape)

    # rank 0 fills the array by data from random vlaues generator
    # note: we are really bad people here when leaving all work on
    # rank 0. We can be parallel here of course :-)
    if comm.Get_rank() == 0:
        rng = np.random.default_rng()
        rng.random(size=dataset_shape, dtype=np.float64, out=dataset)

    # all ranks wait until rank 0 is finished
    comm.Barrier() 
else:
    # non-MPI version
    dataset = np.random.random(dataset_shape, dtype=np.float64)

chunks = (25, 1000, 1000)
h5_name = '/scratch/tmp/APS_sample_parallel.h5'
zarr_name = '/scratch/tmp/APS_sample_parallel.zarr'

# MPI HDF5 test - we do it first as the earlier test has an advantage of a free cache :-)
if MPI is not None:
    if MPI.COMM_WORLD.Get_rank()==0:
        os.system('rm -rf {h5}'.format(h5=h5_name))  
        print(dataset.size*dataset.dtype.itemsize/1000**2, 'MB')
    t = time.time()
    write_to_hdf5p_parallel(dataset, h5_name, chunks)
    if MPI.COMM_WORLD.Get_rank()==0:
        print('h5p num_workers={}: {}s'.format(MPI.COMM_WORLD.Get_size(), time.time() - t))

# with MPI only rank=0 is doing zarr and hdf5 serial tests
if MPI is None or MPI.COMM_WORLD.Get_rank()==0:
    for num_workers in [1, 2, 4, 8, 16]:
        os.system('rm -rf {h5} {zarr}'.format(h5=h5_name, zarr=zarr_name))
        t = time.time()
        write_to_zarr_parallel(dataset, zarr_name, chunks, num_workers)
        print('zarr num_workers={}: {}s'.format(num_workers, time.time() - t))
        t = time.time()
        write_to_hdf5_parallel(dataset, h5_name, chunks, num_workers)
        print('h5 num_workers={}: {}s'.format(num_workers, time.time() - t))
        print('')

