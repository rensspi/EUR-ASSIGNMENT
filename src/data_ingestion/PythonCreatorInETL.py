import fme
from fme import BaseTransformer
import fmeobjects
import arcpy #this is ESRI's arcpy Package, created to deal with ESRI-specific formats


class FeatureCreator(BaseTransformer):
    """Template Class Interface:
    When using this class, make sure its name is set as the value of the 'Class to Process Features'
    transformer parameter.

    This class inherits from 'fme.BaseTransformer'. For full details of this class, its methods, and
    expected usage, see https://docs.safe.com/fme/html/fmepython/api/fme.html#fme.BaseTransformer.
    """

    def __init__(self):
        """Base constructor for class members."""
        pass

    def input(self, feature: fmeobjects.FMEFeature):
        """This method is called once by FME to initiate feature creation.
        Any number of features can be created and emitted by the self.pyoutput() method.
        The initial feature argument is a placeholder and can be ignored.
        """
        SDE_bestand = FME_MacroValues['SDE_bestand']
        print(SDE_bestand)
        
        arcpy.env.workspace = SDE_bestand
        
        print(arcpy.env.workspace)
        
        Tables = arcpy.ListFeatureClasses()
        Tables += arcpy.ListTables()
        
        ArchivedTables = []
        
        for i in Tables:
           try:
              desc = arcpy.Describe(i)
              if desc.isArchived:
                  ArchivedTables.append(i)
           except:
              print("{} niet meegenomen".format(i))

        for i in ArchivedTables:
            newFeature = fmeobjects.FMEFeature()
            newFeature.setAttribute("table", i)
#            desc = arcpy.da.Describe(i)
#            if 'shapeType' in desc.keys():
#                newFeature.setAttribute("geometry_type", desc['shapeType'])
            self.pyoutput(newFeature)
 
    def close(self):
        """This method is called at the end of the class."""
        pass
