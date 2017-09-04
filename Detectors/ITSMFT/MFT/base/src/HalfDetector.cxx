// Copyright CERN and copyright holders of ALICE O2. This software is
// distributed under the terms of the GNU General Public License v3 (GPL
// Version 3), copied verbatim in the file "COPYING".
//
// See http://alice-o2.web.cern.ch/license for full licensing information.
//
// In applying this license CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

/// \file HalfDetector.cxx
/// \brief Class Building the geometry of one half of the ALICE Muon Forward Tracker
/// \author Raphael Tieulent <raphael.tieulent@cern.ch>

#include "TGeoMatrix.h"

#include "FairLogger.h"

#include "MFTBase/HalfDiskSegmentation.h"
#include "MFTBase/HalfSegmentation.h"
#include "MFTBase/HalfDisk.h"
#include "MFTBase/Geometry.h"
#include "MFTBase/HalfDetector.h"

using namespace o2::MFT;

ClassImp(o2::MFT::HalfDetector)

/// \brief Default constructor

//_____________________________________________________________________________
HalfDetector::HalfDetector():
TNamed(),
mHalfVolume(nullptr),
mSegmentation(nullptr)
{
  
}

/// \brief Constructor

//_____________________________________________________________________________
HalfDetector::HalfDetector(HalfSegmentation *seg):
TNamed(),
mHalfVolume(nullptr),
mSegmentation(seg)
{
  
  Geometry * mftGeom = Geometry::instance();
  
  SetUniqueID(mSegmentation->GetUniqueID());
  
  SetName(Form("MFT_H_%d",mftGeom->getHalfMFTID(GetUniqueID())));
    
  Info("HalfDetector",Form("Creating : %s ",GetName()),0,0);

  mHalfVolume = new TGeoVolumeAssembly(GetName());
  
  createHalfDisks();

}

//_____________________________________________________________________________
HalfDetector::~HalfDetector() 
= default;

/// \brief Creates the Half-disks composing the Half-MFT 

//_____________________________________________________________________________
void HalfDetector::createHalfDisks()
{

  Info("CreateHalfDisks",Form("Creating  %d Half-Disk ",mSegmentation->getNHalfDisks()),0,0);
  
  for (Int_t iDisk = 0 ; iDisk < mSegmentation->getNHalfDisks(); iDisk++) {
    HalfDiskSegmentation * halfDiskSeg = mSegmentation->getHalfDisk(iDisk);    
    auto * halfDisk = new HalfDisk(halfDiskSeg);
    Int_t halfDiskId = Geometry::instance()->getHalfDiskID(halfDiskSeg->GetUniqueID());
    mHalfVolume->AddNode(halfDisk->getVolume(),halfDiskId,halfDiskSeg->getTransformation());
    delete halfDisk;
  }
  
}
