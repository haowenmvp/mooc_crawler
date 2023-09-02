from datetime import datetime


class ResourceStructureInfo:

    def __init__(self):
        self.resource_structure_id = 0
        self.semester_id = 0
        self.resource_chapter_index = 0
        self.resource_chapter_name = ''
        self.resource_knobble_index = 0
        self.resource_knobble_name = ''
        self.update_time = datetime(1999, 1, 1)


class ResourceInfo:

    class ResourceTypeEnum:
        kTypeUnknown = 0
        kVideo = 1
        kDocument = 2
        kTest = 3
        kTypeOther = 4

    def __init__(self):
        self.resource_id = 0
        self.resource_name = ''
        self.resource_type = ResourceInfo.ResourceTypeEnum.kTypeUnknown
        self.resource_structure_id = ''
        self.resource_teacherid = ''
        self.resource_teacher = ''
        self.resource_time = 0
        self.resource_storage_location = ''
        self.resource_network_location = ''
        self.update_time = datetime(1999, 1, 1)
