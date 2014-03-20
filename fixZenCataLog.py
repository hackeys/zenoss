#!/usr/bin/env python
import Globals
from Products.ZenUtils.ZenScriptBase import ZenScriptBase
from transaction import commit
dmd = ZenScriptBase(connect=True).dmd

class ZenCatalogCheck:
    """
    Detects and fixes a variety of potentials problems in the ZODB catalogs.
    """

    def run(self, fix=True):
        self.fixCatalogs(fix)
        if fix == True:
            self.reindex()


    def fixCatalogs(self, Fix):
        def fixCatalog(catalog, Fix):
            try:
                print "  * %s" % catalog.id
                removeCount = 0
                problems = True
                while problems:
                    problems = False
                    brains = catalog()
                    for o in brains:
                        try:
                            bah = o.getObject()
                        except Exception:
                            if Fix == True:
                                print "    - removing %s." % o.getPath()
                                catalog.uncatalog_object(o.getPath())
                                removeCount += 1
                                problems = True
                            else:
                                print "    - Should be removed from catalog %s." % o.getPath()
                                problems = False
                    if Fix == True:
                        commit()
                if removeCount > 0: print
            except AttributeError:
                print "{} not found.  skipping....".format(catalog)
            except Exception:
                raise

        print "Cleaning up invalid entries from catalogs:"
        for catalog in (
            dmd.global_catalog,
            dmd.Devices.deviceSearch,
            dmd.ZenLinkManager.layer3_catalog,
            dmd.maintenanceWindowSearch,
            dmd.zenPackPersistence,
            dmd.Manufacturers.productSearch,
           ):
            fixCatalog(catalog, Fix=Fix)
        print


    def reindex(self):
        print "Reindexing Devices"
        dmd.Devices.reIndex()
        commit()
        print "Reindexing Events"
        dmd.Events.reIndex()
        commit()
        print "Reindexing Manufacturers"
        dmd.Manufacturers.reIndex()
        commit()
        print "Reindexing Networks"
        dmd.Networks.reIndex()
        commit()


if __name__ == "__main__":
    zfi = ZenCatalogCheck()
    zfi.run(fix=True)

