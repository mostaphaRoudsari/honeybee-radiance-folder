"""Utility functions for sensor grids folder."""
import os
import json


def distribute_sensors(
        input_folder, output_folder, output_grid_count, min_sensor_count=2000):
    """Evenly distribute Sensor grids among new sensor grid files.

    This function creates a new folder with sensor grids and a _dist_info.json file
    which has the information to redistribute the generated files to create the original
    input files.

    Args:
        input_folder: Input sensor grids folder.
        output_folder: A new folder to write the newly created files.
        output_grid_count: Number of output sensor grids to be created. This number
            is usually equivalent to the number of process that can be used to run
            the simulations in parallel.
        min_sensor_count: Minimum number of sensors in each output grid. If the number
            of grids in each output grid becomes smaller than this number the
            output_grid_count will be adjusted to satisfy this minimum. To ignore this
            limitation set the value to 1. Default: 2000.

    """
    info_file = os.path.join(input_folder, '_info.json')
    with open(info_file) as inf:
        data = json.load(inf)
    total_count = sum(grid['count'] for grid in data)
    sensor_per_grid = int(round(total_count / output_grid_count)) or 1
    if sensor_per_grid < min_sensor_count:
        # re-calculate based on minimum sensor counts
        output_grid_count = int(round(total_count / min_sensor_count))
        sensor_per_grid = int(round(total_count / output_grid_count))

    input_grid_files = [
        os.path.join(input_folder, f) for f in os.listdir(input_folder)
        if f.endswith('.pts')
    ]

    dist_info = [{'name': name, 'dist_info': []} for name in os.listdir(input_folder)]
    input_grids_iter = iter(input_grid_files)

    def get_next_input_grid():
        return open(next(input_grids_iter))

    def get_target_file(index):
        outf = os.path.join(output_folder, '%d.pts' % index)
        return open(outf, 'w')

    outf_index = 0
    outf = get_target_file(outf_index)
    line_out_count = 0
    for i in range(len(input_grid_files)):
        inf = get_next_input_grid()
        dist_info[i]['dist_info'].append({'name': outf_index, 'st_ln': line_out_count})
        for line in inf:
            line_out_count += 1
            outf.write(line)
            if line_out_count % sensor_per_grid == 0:
                # add out information for input file
                dist_info[i]['dist_info'][-1]['end_ln'] = line_out_count - 1
                outf_index += 1
                if outf_index == output_grid_count:
                    # This was the last file. Add the remainder of the data
                    outf.write(line)
                    for line in inf:
                        line_out_count += 1
                        outf.write(line)
                    inf.close()
                    dist_info[i]['dist_info'].append(
                        {'name': outf_index, 'st_ln_': line_out_count}
                    )
                    break
                line_out_count = 0
                # open a new file
                outf.close()
                outf = get_target_file(outf_index)
                dist_info[i]['dist_info'].append(
                    {'name': outf_index, 'st_ln': line_out_count}
                )
        dist_info[i]['dist_info'][-1]['end_ln'] = line_out_count - 1
        inf.close()

    dist_info_file = os.path.join(output_folder, '_dist_info.json')
    with open(dist_info_file, 'w') as dist_out_file:
        json.dump(dist_info, dist_out_file)


def recreate_grids(input_folder, output_folder):
    _dist_info_file = os.path.join(input_folder, '_dist_info.json')
    with open(_dist_info_file) as inf:
        data = json.load(inf)

    for f in data:
        out_file = os.path.join(output_folder, f['name'])
        with open(out_file, 'w') as outf:
            for src_info in f['dist_info']:
                src_file = os.path.join(input_folder, str(src_info['name']) + '.pts')
                st = src_info['st_ln']
                end = src_info['end_ln']
                with open(src_file) as srf:
                    for _ in range(st):
                        next(srf)
                    for _ in range(end - st + 1):
                        outf.write(next(srf))
