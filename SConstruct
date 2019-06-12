
import glob

env = Environment()

env.AppendUnique(CPPPATH=['.'])
env.AppendUnique(CXXFLAGS=['--std=c++11', '-g'])
env.AppendUnique(LIBS=['ibverbs'])

env.Program('hdrdma', glob.glob('*.cc'))

