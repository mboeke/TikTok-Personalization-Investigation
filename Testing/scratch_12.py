from collections import namedtuple


class Animal:
    def __init__(self, n_legs: int):
        self.n_legs: int = n_legs

    def make_noise(self):
        raise NotImplementedError()


class Dog(Animal):
    def __init__(self, breed: str, *args, **kwargs):
        print(args)
        print(kwargs)
        self.breed: str = breed
        super().__init__(*args, **kwargs)


Point = namedtuple("Point", field_names=("x", "y", "z"))


if __name__ == '__main__':
    dog = Dog("labrador", 4)
    print(dir(dog))

    cat = Animal(2)
    print(cat.n_legs)

    p = Point(0.2, 0.2, 0.3)
    print(dir(p))