
import glob

#env = Environment(CXX='/apps/gcc/4.9.2/bin/g++')

env.AppendUnique(CPPPATH=['.'])
env.AppendUnique(CXXFLAGS=['--std=c++11', '-g'])
env.AppendUnique(LIBS=['ibverbs','z'])

env.Program('hdrdmacp', glob.glob('*.cc'))

