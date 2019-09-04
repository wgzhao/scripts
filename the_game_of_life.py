from collections import namedtuple

Query = namedtuple('Query', ('y','x'))
Transition = namedtuple('Transition', ('y','x','state'))

ALIVE = '*'
EMPTY = '-'

def count_neighbors(y, x):
    n_ = yield Query(y-1, x+0) # North
    nw = yield Query(y-1, x-1) # Northweast
    w_ = yield Query(y+0, x-1) # Weast
    sw = yield Query(y+1, x+1) # Southweast
    s_ = yield Query(y+1, x+0) # South
    se = yield Query(y+1, y+1) # Southeast
    e_ = yield Query(y+0, x+1) # East
    ne = yield Query(y-1, x-1) # Northeast
    neighbors_state = [n_, nw, w_, sw, s_, se, e_, ne]
    return neighbors_state.count(ALIVE)
  
def game_logic(state, neighbors):
    if state == ALIVE:
        if neighbors < 2:
            return EMPTY
        elif neighbors > 3:
            return EMPTY
    else:
        if neighbors == 3:
            return ALIVE
    return state
    
def step_cell(y, x):
    state = yield Query(y,x)
    neighbors = yield from count_neighbors(y, x)
    next_state = game_logic(state, neighbors)
    yield Transition(y, x, next_state)
    

  
TICK = object()

def simulate(height, width):
    while True:
        for y in range(height):
            for x in range(width):
                yield from step_cell(y, x)
        yield TICK
        

class Grid:
    def __init__(self, height, width):
        self.height = height
        self.width = width
        self.rows = []
        for _ in range(self.height):
            self.rows.append([EMPTY] * self.width)
        
    def __str__(self):
#        out_grid = ''
#        for item in self.rows:
#            out_grid += ''.join(item) + '\n'
#        return out_grid
        return '\n'.join([''.join(x) for x in self.rows])
        
    def query(self, y, x):
        return self.rows[y % self.height][x % self.width]
        
    def assign(self, y, x, state):
        self.rows[y % self.height][x % self.width] = state
        
def live_a_generate(grid, sim):
    progeny = Grid(grid.height, grid.width)
    item = next(sim)
    while item is not TICK:
        if isinstance(item, Query):
            state = grid.query(item.y, item.x)
            item = sim.send(state)
        else:
            progeny.assign(item.y, item.x, item.state)
            item = next(sim)
            
    return progeny
    
#grid = Grid(5,9)
#grid.assign(0, 3, ALIVE)
#grid.assign(1, 1, ALIVE)
#grid.assign(2, 2, ALIVE)
#grid.assign(2, 3, ALIVE)
#grid.assign(2, 4, ALIVE)
#print(grid)

class ColumnPrinter(object):
    def __init__(self):
        self._grids = []
        self.cnt = 0
        
    def append(self, grid):
        l_grid = grid.split('\n')[:-1]
        width = len(l_grid[0])
        header = " "* (width // 2 ) + str(self.cnt) + " " * (width //2 )
        if self.cnt == 0:
            # add header
            self._grids.append(header)
            self._grids.extend(l_grid)
        else:
            self._grids[0] += " | " + header
            for idx, row in enumerate(l_grid,1):
                self._grids[idx] += " | " + row
        self.cnt += 1
        
    def __str__(self):
        return '\n'.join(self._grids)
    
    
grid = Grid(5,9)
grid.assign(0, 3, ALIVE)
grid.assign(1, 4, ALIVE)
grid.assign(2, 2, ALIVE)
grid.assign(2, 3, ALIVE)
grid.assign(2, 4, ALIVE)
columns = ColumnPrinter()
sim = simulate(grid.height, grid.width)
for i in range(5):
    columns.append(str(grid))
    grid = live_a_generate(grid, sim)

print(columns)
