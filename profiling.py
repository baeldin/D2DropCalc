import cProfile
import pstats
from io import BytesIO as StringIO
import calc


def my_func():
    result = []
    for i in range(10000):
        result.append(i)

    return result

pr = cProfile.Profile()
pr.enable()

calc.main()

pr.disable()
s = StringIO()
print(s)
ps = pstats.Stats(pr, stream=s).sort_stats('tottime')
ps.print_stats()

with open('test.txt', 'w+') as f:
    f.write(s.getvalue())