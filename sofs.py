#!/bin/env python
# -*- coding: utf-8 -*-
import FTPGetter, threading

if __name__ == "__main__":
    
    getter = FTPGetter.FTPGetter()
    getter.processDataset("ftp://ftp.bom.gov.au/register/bom404/outgoing/IMOS/IMG", "/mnt/imos-t3/sofs.emii.org.au_site/sofs/images/",'gif','bom404','Vee8soxo', True, False, False)
    getter.close()

