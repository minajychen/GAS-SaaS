# Genomics Annotation Service 
Work in Progress, complete date ETA: end of March 2023

This is a SaaS application for genomics annotation that leverages the AnnTools package (https://github.com/mpcs-cc/anntools).

At the current phase, a user submits a .vcf file on http://jchen201-a9-web.ucmpcs.org:5000/annotate, and receives processed, annotation files in a private, designated AWS S3.

Once the SaaS application is fully completed by end of March 2023, the decoupled cloud computing architecture will look like the below.

![gas-arch](https://user-images.githubusercontent.com/86486074/218222703-374955a3-ad4c-42a0-bdc3-4bc5a6d5b51f.png)

(the above architecture is authored by Vas Vasiliadis)
