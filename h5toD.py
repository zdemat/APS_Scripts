import h5py
import argparse
import numpy as np

def data_scope(assigned_title):
    """
    Extracts relevant data scope from the assigned title.

    Parameters:
        assigned_title (str): Title assigned to a dataset.

    Returns:
        str: Comma-separated list of matching keywords in the title, or 0 if no matches found.
    """
    keywords = [b'dark', b'flat', b'projections']  # Use bytes-like objects for keywords
    matching_keywords = [keyword.decode('utf-8') for keyword in keywords if keyword in assigned_title.lower()]
    if matching_keywords:
        return ', '.join(matching_keywords)
    else:
        return 0

def ReadDatasets(group, datasets, param):
    """
    Recursively reads datasets within an HDF5 group, identifies 3D arrays, and extracts relevant information.

    Parameters:
        group (h5py.Group): HDF5 group to explore.
        datasets (list): List to store identified datasets along with their associated group and type.
        param (dict): Dictionary to store additional parameters like scan_range, half_acquisition, and npoints.
    """
    for name, item in group.items():
        if isinstance(item, h5py.Group):
            # Recursively search within groups
            ReadDatasets(item, datasets, param)
                
        elif isinstance(item, h5py.Dataset) and item.ndim == 3 and name == 'image':
            path = '/' + str(group).split('/')[1] + '/title'
            
            try:
                title_dataset = group.file[path]
                typeD = data_scope(title_dataset[()])

                if typeD != 0:
                    datasets.append((item, group, typeD))
            except KeyError:
                print(f"Dataset '{path}' not found.")
        elif name == 'scan_range':
                param['scan_range'] = item[()]
        elif name == 'half_acquisition':
                param['half'] = item[()]
        elif name == 'npoints':
                param['angles'] = item[()]

def APS_format(input_file, new_file_path):
    """
    Converts ESRF to APS IMG data structure.

    Parameters:
        input_file (str): Path to the existing HDF5 file (ESRF format).
        new_file_path (str): Path to the new HDF5 file (APS IMG format).
    """
    with h5py.File(input_file, 'r') as input_file, h5py.File(new_file_path, 'w') as outfile:
        # Create groups
        data_g = outfile.create_group('/exchange')
        defa = outfile.create_group('/default')
        meas = outfile.create_group('/measurement')
        proc = outfile.create_group('/process')

        # Use the previous function to identify 3D arrays and 'title' entries
        datasets_list = []
        param = {}
        ReadDatasets(input_file, datasets_list, param)

        for dataset, group, measurement in datasets_list:
            target_path = dataset.name
            
            if measurement == 'projections':
                virtual_source = dataset.virtual_sources()
                data_g.create_dataset(f'data', data=dataset)
            elif measurement == 'flat':
                try:
                    data_g.create_dataset(f'data_white', data=dataset)
                except ValueError:
                    data_g.create_dataset(f'data_whiteF', data=dataset)
            elif measurement == 'dark':
                data_g.create_dataset(f'data_dark', data=dataset)
                
        # Create angle array
        data_g.create_dataset(f'theta', data=np.linspace(0, int(param['scan_range']), int(param['angles'])))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert ESRF to APS IMG data structure.')
    parser.add_argument('-i', dest='fname', required=True, help='Path to the existing HDF5 file.')
    parser.add_argument('-o', dest='outname', required=True, help='Path to the new HDF5 file.')

    args = parser.parse_args()
    APS_format(args.fname, args.outname)

