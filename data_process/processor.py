from persistence.model.process_config import ProcessConfig
from persistence.db.course_list_info_repo import CourseListInfoRepository
from persistence.db.course_group_repo import CourseGroupRepository
from persistence.db.platform_resource_repo import PlatformResourceRepository
from persistence.db.school_resource_repo import SchoolResourceRepository
from data_process.processors.process_schoolinfo import SchoolinfoProcessor
from data_process.processors.process_platforminfo import PlatforminfoProcessor
from data_process.processors.process_course_group import CourseGroupProcessor
from data_process.processors.process_all_platformcourse import ProcessorAllPlatform


class Processor:
    def __init__(self, config: ProcessConfig):
        self.repo = config.repo
        self.course_list_repo = CourseListInfoRepository(**self.repo)
        self.course_group_repo = CourseGroupRepository(**self.repo)
        self.platform_info_repo = PlatformResourceRepository(**self.repo)
        self.school_info_repo = SchoolResourceRepository(**self.repo)

    def run(self):
        ProcessorAllPlatform(ProcessConfig()).run()
        # course_group_processor = CourseGroupProcessor(self)
        # platforminfo_processor = PlatforminfoProcessor(self)
        # schoolinfo_processor = SchoolinfoProcessor(self)
        # course_group_processor.run()
        # platforminfo_processor.run()
        # schoolinfo_processor.run()


if __name__ == '__main__':
    config = ProcessConfig()
    processor = Processor(config)
    processor.run()