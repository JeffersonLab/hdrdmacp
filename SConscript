

import sbms

# get env object and clone it
Import('*')
env = env.Clone()

env.AppendUnique(CXXFLAGS=['--std=c++11', '-g'])
env.AppendUnique(LIBS=['ibverbs','z'])

sbms.executable(env)


