import os
import logging
import traceback
from collections import namedtuple

from flo.builder import WorkflowNotReady
from timeutil import TimeInterval, datetime, timedelta

# every module should have a LOG object
LOG = logging.getLogger(__name__)


class DeltaCatalog(object):

    #collection = {'HIR1B': 'ILIAD',
                  #'CFSR': 'DELTA',
                  #'PTMSX': 'ILIAD'}
    #input_data = {'HIR1B': '/mnt/sdata/geoffc/HIRS_processing/Work/HIR1B.out.sync',
                  #'CFSR':  '/mnt/sdata/geoffc/HIRS_processing/Work/CFSR.out',
                  #'PTMSX': '/mnt/sdata/geoffc/HIRS_processing/Work/PTMSX.out.sync'
                  #}

    def __init__(self, **kwargs):

        self.collection = kwargs['collection']
        self.input_data = kwargs['input_data']
        self.file_data = {}
        self.indexed_file_types = {}

    def process_metadata(self, file_type):
        '''
        Run through the *.out file and create a nested dictionary containing the
        the required metadata for the files of each file_type.
        '''
        LOG.debug("Reading the metadata file...")

        metadata_file = self.input_data[file_type]
        LOG.debug("For file_type = '{}', metadata_file = {}".format(file_type,metadata_file))

        # Open the metadata file and read each line, splitting into the required 
        # metadata for each file.
        with open(metadata_file) as metadata:
            for line in metadata:
                (size, mod_time, begin_time, end_time, sensor, sat, file_type, file_system,
                 relative_path) = line.split(',')
                begin_time = datetime.fromtimestamp(float(begin_time))
                end_time = datetime.fromtimestamp(float(end_time))
                relative_path = relative_path.split()[0]
                name = os.path.basename(relative_path)

                if sensor not in self.file_data:
                    self.file_data[sensor] = {}

                if sat not in self.file_data[sensor]:
                    self.file_data[sensor][sat] = {}

                if file_type not in self.file_data[sensor][sat]:
                    self.file_data[sensor][sat][file_type] = {}

                if name not in self.file_data[sensor][sat][file_type]:

                    if end_time < begin_time:
                        print name

                    self.file_data[sensor][sat][file_type][name] = {
                        'data_interval': TimeInterval(begin_time, end_time),
                        'name': name,
                        'path': relative_path}

        self.indexed_file_types[file_type] = 1


    def check_file_index(self, file_type):

        if file_type not in self.indexed_file_types:
            self.process_metadata(file_type)


    def file(self, sensor, sat, file_type, begin_time):

        file_list = self.files(sensor, sat, file_type, TimeInterval(begin_time, begin_time))
        LOG.debug("file_list: {}".format(file_list))

        # Making sure we have the right begin time as some inputs overlap
        for (i, file) in enumerate(file_list):
            if file.data_interval.left == begin_time:
                return file_list[i]

        raise WorkflowNotReady('No files for {} {} {} {}'.format(sensor, sat, file_type,
                                                                  begin_time))

    def files(self, sensor, sat, file_type, target_interval):

        LOG.debug('sensor = {}'.format(sensor))
        LOG.debug('sat = {}'.format(sat))
        LOG.debug('file_type = {}'.format(file_type))
        LOG.debug('target_interval = {}'.format(target_interval))

        # Loading files of type file_type before searching
        self.check_file_index(file_type)

        # Get the dict of the file metadata for the correct sensor, satellite and 
        # file type...
        if file_type not in self.file_data[sensor][sat]:
            raise WorkflowNotReady('No files for {} {} {} {}'.format(sensor, sat, file_type,
                                                                      target_interval.left))
        else:
            file_search = self.file_data[sensor][sat][file_type]
            fs_keys = file_search.keys()
            fs_keys.sort()
            #print('file_search[{}] = {}'.format(fs_keys[0],file_search[fs_keys[0]]))
            #print('file_search = {}'.format(file_search))

        #for key in fs_keys:
            #print('{}'.format(file_search[key]))

        # Create a list of all of the files which overlap the desired time interval.
        files = [self.file_info(file_search[name], file_type)
                 for name in file_search.keys()
                 if target_interval.overlaps(file_search[name]['data_interval'])]

        # Remove any duplicates from the file list.
        return self.remove_duplicates(files)

    def remove_duplicates(self, files):

        prune_files = {}

        for file in files:
            # This granule start time has already been encountered...
            if file.data_interval.left in prune_files:

                old_file = prune_files[file.data_interval.left]
                new_file = file

                # Determine the durations of the old and new granules...
                old_duration = old_file.data_interval.duration
                new_duration = new_file.data_interval.duration

                # Put new granule with this start time if its duration is longer
                if new_duration > old_duration:
                    print 'For start time ({}), replacing the end time ({}) ({}) with new end time ({}) ({}).'.format(
                            old_file.data_interval.left,
                            old_file.data_interval.right,old_file.name,
                            new_file.data_interval.right,new_file.name)

                    prune_files[file.data_interval.left] = new_file
                else:
                    print 'For start time ({}), retaining the end time ({}) ({}), discarding the new end time ({}) ({}).'.format(
                            old_file.data_interval.left,
                            old_file.data_interval.right,old_file.name,
                            new_file.data_interval.right,new_file.name)
            else:
                # Haven't seen this granule start time before, add to the dict...
                prune_files[file.data_interval.left] = file

        # Sort the list of dbase entries based on filename.
        delta_list = prune_files.values()
        delta_key = lambda x: (x.name)
        delta_list_sorted = sorted(delta_list,key=delta_key)

        #return prune_files.values()
        return delta_list_sorted


    def file_info(self, line, file_type):

        LOG.debug('line = {}'.format(line))
        LOG.debug('file_type = {}'.format(file_type))
        LOG.debug('self.collection["{}"] = {}'.format(file_type, self.collection[file_type]))
        return DeltaFile(name=line['name'],
                         data_interval=line['data_interval'],
                         collection=self.collection[file_type],
                         path=line['path'])

DeltaFile = namedtuple('DeltaFile', ['name', 'data_interval', 'collection', 'path'])
#delta_catalog = DeltaCatalog()
