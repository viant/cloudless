# Data synchronizer

Data Synchronizer updates application objects from a storage. A process checks if underlying storage object 
has been modified and if so it starts synchronization. 

Large amount of data synchronization be both memory and CPU extensive, causing application performance hit got short duration.
In case where only small amount of actual changes are submitted this package allows to 
selectively applies these changes with reusing non mutated state.

To allow selective changes, snapshoter and keyer has to be implemented to retrieve a record key from byte array.
Make sure this function is substantially cheaper than the record decoding function.
