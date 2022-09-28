import numpy as np
import matplotlib.pyplot as plt
x = np.array([1,2,3,4,5,6,7,8])
y = x

#plt.figure()
#plt.scatter(x,y)
#plt.show()

colors = ['green']* (len(x)-1)
colors.append('red')
plt.figure()
plt.scatter(x,y, s= 100, c = colors)
plt.show()