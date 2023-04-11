class ScalableGenerator:
    base_factor = 0

    @classmethod
    def scale(cls, scale_factor: int):
        raise NotImplemented


class LinearScale(ScalableGenerator):
    @classmethod
    def scale(cls, scale_factor: int):
        return cls.base_factor * scale_factor


class ConstantScale(ScalableGenerator):
    @classmethod
    def scale(cls, scale_factor: int):
        return cls.base_factor
