import simpy
import random
import itertools

# total simulation time in seconds
T_SIM = 3600 * 3
# random seed
RANDOM_SEED = 42
# number of scooters that can use the station at once
N_SCOOTER = 1
# time to replace battery
T_REPLACE = 300
# time for new scooter to appear in seconds
T_SCOOTER = [900, 1800]
# mAH / 100 * Panasonic 18650@ 3400mAH
BATTERY_CAPACITY = 340000
# battery level of scooter when arriving at station
BAT_LVL_SCOOTER = [6800, 51000]
# station battery charging speed mAH / second
CHR_T = 190
# patience of driver, willingness to wait
SCOOTER_PATIENCE = [60, 300]

# Defines how many scooters can use the station at once in N_SCOOTER
# Sets battery capacity defined in BATTERY_CAPACITY
# Replacement process steps


class Station(object):

    def __init__(self, env):
        self.env = env
        self.station = simpy.Resource(env, capacity=N_SCOOTER)
        self.battery = simpy.Container(env, capacity=BATTERY_CAPACITY,
                                       init=BATTERY_CAPACITY)

    def replace(self, name, bat_lvl_scooter):
        new_battery_lvl = self.battery.level - bat_lvl_scooter
        self.battery.get(new_battery_lvl)
        yield env.timeout(T_REPLACE)
        print('{name} - replaced battery and left - time:{time:1.2f}'.
              format(name=name, time=env.now))

    def recharge(self):
        print('station battery started recharging - time:{time:1.2F}'.
              format(time=env.now))
        start = env.now
        recharge_amount = self.battery.capacity - self.battery.level
        self.battery.put(recharge_amount)
        yield env.timeout(recharge_amount / CHR_T)
        recharge_duration = env.now - start
        print('station battery fully charged in {dur:1.2f} - time:{time:1.2f}'.
              format(dur=recharge_duration, time=env.now))


# Scooter requests station use
# Generates battery level of scooter on arrival


def scooter(env, name, station):

    bat_lvl_scooter = random.randint(*BAT_LVL_SCOOTER)
    print('{name} - arrives at station with {lvl} mAH - time:{time:1.2f}'.
          format(name=name, time=env.now, lvl=bat_lvl_scooter))

    with station.station.request() as req:
        patience = random.uniform(*SCOOTER_PATIENCE)
        results = yield req | env.timeout(patience)

        if req in results:
            yield env.process(station.replace(name, bat_lvl_scooter))
            yield env.process(station.recharge())
        else:
            print('{name} - lost patience and left - time:{time:1.2f}'.
                  format(name=name, time=env.now))


# Continuously generates scooter processes until simulation time ends
# Interval (min, max) for scooter generation is defined in T_SCOOTER


def generate_scooter(env):
    station = Station(env)
    for i in itertools.count():
        yield env.timeout(random.randint(*T_SCOOTER))
        env.process(scooter(env, 'Scooter {}'.format(i), station))


random.seed(RANDOM_SEED)

env = simpy.Environment()

env.process(generate_scooter(env))

env.run(until=T_SIM)
