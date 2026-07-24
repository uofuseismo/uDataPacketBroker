# About

This is a data packet broker (middleware) which is typically deployed as a stateful set.  It supports two use cases as shown in the
diagram.


<img width="932" height="391" alt="brokerbridge drawio" src="https://github.com/user-attachments/assets/1d8331f3-6c3d-49df-8019-a31ed6eedb74" />


On the left, it acts as buffer for when an individual transport experiences a backfill event.  In this case, the number of read data
packets typically spikes.  On the right, it acts as an aggregation point for various import mechanisms so that downstream consumers can
point to a single service for seismic data packets.  If handling backfill spikes is not a concern, then one data packet broker is sufficient
that acts as a common aggregation point, a single service subsequent packets, and a buffer for data surges.
