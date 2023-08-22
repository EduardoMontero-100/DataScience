## The train and test subdirectories are already created
#import os
#
#base_dir = 'cats_and_dogs_filtered'
#
#print('Content of base directory')
#print(os.listdir(base_dir))
#
#print('\nContent of the train directory')
#print(os.listdir(f'{base_dir}/train'))
#
#print('\nContent of the test directory')
#print(os.listdir(f'{base_dir}/validation'))
#
## We can assign each of these directories to a variable so you can use it later.
#import os
#
#train_dir = os.path.join(base_dir, 'train')
#validation_dir = os.path.join(base_dir, 'validation')
#
## Directory with training cats/dogs pictures
#train_cats_dir = os.path.join(train_dir, 'cats')
#train_dogs_dir = os.path.join(train_dir, 'dogs')
#
## Directory with validation cats/dogs pictures
#validation_cats_dir = os.path.join(validation_dir, 'cats')
#validation_dogs_dir = os.path.join(validation_dir, 'dogs')
#
#train_cat_fnames = os.listdir(train_cats_dir)
#train_dog_fnames = os.listdir(train_dogs_dir)
#
#validation_cat_fnames = os.listdir(validation_cats_dir)
#validation_dog_fnames = os.listdir(validation_dogs_dir)
#
#
#print(f'total training cat images: ', len(train_cat_fnames))
#print(f'total training dog images: ', len(train_dog_fnames))
#
#print(f'total validation cat images: ', len(validation_cat_fnames))
#print(f'total validation dog images: ', len(validation_dog_fnames))
#
##%matplotlib notebook
#
#import matplotlib.image as mpimg
#import matplotlib.pyplot as plt
#
## Parameters for our graph; we will output images in a 4*4 configuration
#nrows = 4
#ncols = 4
#
#pic_index = 0 # Index for iterating over images
#
## Set up matplotlib fig, and size it to fit 4*4 pics.
#fig = plt.gcf()
#fig.set_size_inches(ncols*4, nrows*4)
#
#pic_index+8
#
#next_cat_pix = [os.path.join(train_cats_dir, fname)
#               for fname in train_cat_fnames[pic_index-8:pic_index]
#               ]
#
#next_dog_pix = [os.path.join(train_dogs_dir, fname)
#               for fname in train_dog_fnames[pic_index-8:pic_index]
#               ]
#
#for i, img_path in enumerate(next_cat_pix+next_dog_pix):
#    # Set up subplot, subplot indices start at 1
#    sp = plt.subplot(nrows, ncols, i+1)
#    sp.axis('Off') # Dont show axes (or gridlines)
#    
#    img = mpimg.imread(img_path)
#    plt.imshow(img)
#    
#plt.show()

from tkinter import *

import matplotlib.pyplot as plt

plt.plot(range(20), range(20))

plt.show()