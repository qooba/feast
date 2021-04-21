class FeastObjectNotFoundException(Exception):
    pass


class EntityNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Entity {name} does not exist in project {project}")
        else:
            super().__init__(f"Entity {name} does not exist")


class FeatureViewNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(f"Feature view {name} does not exist in project {project}")
        else:
            super().__init__(f"Feature view {name} does not exist")


class FeatureTableNotFoundException(FeastObjectNotFoundException):
    def __init__(self, name, project=None):
        if project:
            super().__init__(
                f"Feature table {name} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature table {name} does not exist")


class FeatureBucketNotExist(FeastObjectNotFoundException):
    def __init__(self, bucket, project=None):
        if project:
            super().__init__(
                f"Feature bucket {bucket} does not exist in project {project}"
            )
        else:
            super().__init__(f"Feature bucket {bucket} does not exist")


class FeatureBucketForbiddenAccess(FeastObjectNotFoundException):
    def __init__(self, bucket, project=None):
        if project:
            super().__init__(
                f"Private Registry Bucket {bucket} forbidden Access in project {project}"
            )
        else:
            super().__init__(f"Private Registry Bucket {bucket} forbidden Access")


class FeastProviderLoginError(Exception):
    """Error class that indicates a user has not authenticated with their provider."""
