'use strict';

/* 
*   Drop this file into ./lib of your Serverless service. 
* 
*/
const Util = require('util');
const fs = require('fs');
const cloneDeep = require('lodash/cloneDeep');
const Moment = require('moment');
const MomentTimezone = require('moment-timezone');

const Helpers = require('./helpers.js');

// ============= 
// MODULES 
// ============= 

// ============= 
// CONSTS 
// ============= 

// ============= 
// FUNCTIONS 
// ============= 

const IsBirdEvent = (eventType) => {
  // ----------------------------------------------------   
  // Static method to return whether an event type is a bird event
  switch(eventType) {
    case 'FIRST_MARKING_IN_HAND':
    case 'SIGHTING_BY_PERSON':
    case 'IN_HAND':
    case 'IN_HAND_PRE_CHANGE':
    case 'IN_HAND_POST_CHANGE':
    case 'RECORDED_BY_TECHNOLOGY':
      return true;
    case 'LOST':
    case 'PRACTICE':
    case 'OTHER':
    case 'TRANSFER':
    default:
      return false;
  }
}

const IsTransferEvent = (eventType) => {
  // ----------------------------------------------------   
  // Static method to return whether an event type is a bird event
  switch(eventType) {
    case 'TRANSFER':
      return true;
    case 'LOST':
    case 'PRACTICE':
    case 'OTHER':
    case 'FIRST_MARKING_IN_HAND':
    case 'SIGHTING_BY_PERSON':
    case 'IN_HAND':
    case 'IN_HAND_PRE_CHANGE':
    case 'IN_HAND_POST_CHANGE':
    case 'RECORDED_BY_TECHNOLOGY':
    default:
      return false;
  }
}

const IsInHandEvent = (eventType) => {
  // ----------------------------------------------------   
  // Static method to return whether an event type is a bird event
  switch(eventType) {
    case 'FIRST_MARKING_IN_HAND':
    case 'IN_HAND':
    case 'IN_HAND_PRE_CHANGE':
    case 'IN_HAND_POST_CHANGE':
      return true;
    case 'SIGHTING_BY_PERSON':
    case 'RECORDED_BY_TECHNOLOGY':
    case 'LOST':
    case 'PRACTICE':
    case 'OTHER':
    case 'TRANSFER':
    default:
      return false;
  }
}

const GetRegionFromCode = (regionCode) => {
  // ----------------------------------------------------   
  // Static method to return whether an event type is a bird event
  switch(regionCode) {
    case 'BBR1':
    case 'GBR1':
      return 'NORTHLAND';
    case 'BBR2':
    case 'GBR2':
      return 'AUCKLAND';
    case 'BBR3':
    case 'GBR3':
      return 'WAIKATO';
    case 'BBR4':
    case 'GBR4':
      return 'BAY OF PLENTY';
    case 'BBR5':
    case 'GBR5':
      return 'GISBOURNE';
    case 'BBR6':
    case 'GBR6':
      return 'HAWKE\'S BAY';
    case 'BBR7':
    case 'GBR7':
      return 'TARANAKI';
    case 'BBR8':
    case 'GBR8':
      return 'MANAWATU-WHANGANUI';
    case 'BBR9':
    case 'GBR9':
      return 'WELLINGTON';
    case 'BBR10':
    case 'GBR10':
      return 'WEST COAST';
    case 'BBR11':
    case 'GBR11':
      return 'CANTERBURY';
    case 'BBR12':
    case 'GBR12':
      return 'OTAGO';
    case 'BBR13':
    case 'GBR13':
      return 'SOUTHLAND';
    case 'BBR14':
    case 'GBR14':
      return 'TASMAN';
    case 'BBR15':
    case 'GBR15':
      return 'NELSON';
    case 'BBR16':
    case 'GBR16':
      return 'MARLBOROUGH';
    case 'BBR17':
    case 'GBR17':
      return 'CHATHAMS';
    case 'BBR18':
    case 'GBR18':
      return 'SUBANTARCTIC ISLANDS';
    case 'BBR19':
    case 'GBR19':
      return 'PACIFIC ISLANDS';
    case 'BBR20':
    case 'GBR20':
      return 'AT SEA';
    case 'BBR21':
    case 'GBR21':
      return 'AUSTRALIA';
    case 'BBR22':
    case 'GBR22':
      return 'OTHER';
    default:
      return 'REGION UNKNOWN';
  }
}

const GetCodeFromRegion = (regionName) => {
  // ----------------------------------------------------   
  // Static method to return whether an event type is a bird event
  switch(regionName) {
    case 'NORTHLAND':
      return '1';
    case 'AUCKLAND':
      return '2';
    case 'WAIKATO':
      return '3';
    case 'BAY OF PLENTY':
      return '4';
    case 'GISBOURNE':
      return '5';
    case 'HAWKE\'S BAY':
      return '6';
    case 'TARANAKI':
      return '7';
    case 'MANAWATU-WHANGANUI':
      return '8';
    case 'WELLINGTON':
      return '9';
    case 'WEST COAST':
      return '10';
    case 'CANTERBURY':
      return '11';
    case 'OTAGO':
      return '12';
    case 'SOUTHLAND':
      return '13';
    case 'TASMAN':
      return '14';
    case 'NELSON':
      return '15';
    case 'MARLBOROUGH':
      return '16';
    case 'CHATHAMS':
      return '17';
    case 'SUBANTARCTIC ISLANDS':
      return '18';
    case 'PACIFIC ISLANDS':
      return '19';
    case 'AT SEA':
      return '20';
    case 'AUSTRALIA':
      return '21';
    case 'OTHER':
      return '22';
    default:
      return '-1-';
  }
}

const NormaliseRowMarkConfig = (row, searchPrefix = "out_mark_config", newPrefix = "mark_config", splitRow = false) => {
  // ----------------------------------------------------   
  // Reformat the raw mark config to relate to single event (either in or out)
  var newRow = cloneDeep(row);
  const inProps = Object.keys(newRow).filter(prop => prop.startsWith(searchPrefix));
  inProps.forEach(prop => {
    newRow[prop.replace(searchPrefix, newPrefix)] = newRow[prop];
  });

  // If this row is not being split (i.e. it is not registering a band change),
  // OR, this row is the in_mark_config component of a change event,
  // Tidy-up to remove all other raw mark config keys cause these are no longer needed
  if (!splitRow || searchPrefix === "in_mark_config") {
    // -------------------------------------------------
    newRow = Object.keys(newRow)
      .filter(key => !key.includes('_mark_config_'))
      .reduce((updatedRow, filteredKey) => {
        return {
          ...updatedRow,
          [filteredKey]: newRow[filteredKey]
        };
      }, {});
  }
  // Otherwise - remove only the out_mark_config_... keys because these are duplicates
  else if (splitRow && searchPrefix === 'out_mark_config') {
    // -------------------------------------------------
    newRow = Object.keys(newRow)
      .filter(key => !key.includes('out_mark_config_'))
      .reduce((updatedRow, filteredKey) => {
        return {
          ...updatedRow,
          [filteredKey]: newRow[filteredKey]
        };
      }, {});
  }

  // Lastly, we need to modify the event_type for change events
  // This is so that we are explicit about whether this is pre-change/post-change
  if (splitRow) {
    switch (searchPrefix) {
      case 'in_mark_config': {
        newRow.event_type = 'in-hand-pre-change';
        break;
      }
      case 'out_mark_config': {
        newRow.event_type = 'in-hand-post-change';
        // On a split - ensure characteristic details are only
        // included for the 'in' event, not duplicated for the 'out' event
        newRow = Object.keys(newRow)
          .filter(key => !(key.includes('characteristic_detail_') || key.includes('characteristic_units_') || key.includes('characteristic_value_')))
          .reduce((updatedRow, filteredKey) => {
            return {
              ...updatedRow,
              [filteredKey]: newRow[filteredKey]
            };
          }, {});
          break;
      }
    }
  }

  return newRow;
};

const PreProcessMarkConfigurations = (row) => {
  // ----------------------------------------------------   
  // This functions splits incoming rows based on some criteria.
  // if criteria is not met, the row is returned as-is.
  var rows = [];

  // If the mark_state_code is not null, this is a stock event.
  // No criteria exist for splitting a stock event. Just return a
  // normalised row.
  if (row.mark_state_code) {
    rows.push(NormaliseRowMarkConfig(row, "out_mark_config"));
    return rows;
  }

  // Check the in/out marks data columns.
  // Truth Table:
  // ====================================
  // IN = EMPTY,     OUT = EMPTY :      INVALID 
  // IN = EMPTY,     OUT = NON-EMPTY :  NO SPLIT
  // IN = NON-EMPTY, OUT = NON-EMPTY :  SPLIT
  // IN = NON-EMPTY, OUT = EMPTY :      Use IN values as OUT (Just to help)
  var inMarksEmpty = Object.keys(row).filter(prop => prop.startsWith("in_mark_config")).map(prop => row[prop]).every(value => !value);
  var outMarksEmpty = Object.keys(row).filter(prop => prop.startsWith("out_mark_config")).map(prop => row[prop]).every(value => !value);

  // We've got an IN configuration and an OUT configuration. Split into two rows - this is a change event
  if (!inMarksEmpty && !outMarksEmpty && TransformEventType(row.event_type) === 'IN_HAND') {
    rows.push(NormaliseRowMarkConfig(row, "in_mark_config", "mark_config", true));
    rows.push(NormaliseRowMarkConfig(row, "out_mark_config", "mark_config", true));
  }
  // Check whether we've got an IN config. We'll be kind and use this as the main mark config
  else if (!inMarksEmpty) {
    rows.push(NormaliseRowMarkConfig(row, "in_mark_config"));
  }
  // Otherwise, assume it's a proper OUT mark config, even if the values are NULL  
  else {
    rows.push(NormaliseRowMarkConfig(row, "out_mark_config"));
  }

  // Return our array of rows with normalised mark configurations
  return rows;
}

const processCellData = (ss, rowString, mapping, flatTableColumn) => {
  // ----------------------------------------------------       
  var cellData;

  // cellData handled as ARRAY 
  // Note: ignore unit and data to avoid duplicate processing (these are handled as part of the detail array) 
  if (Array.isArray(mapping[flatTableColumn]) && !['unit', 'data'].includes(flatTableColumn)) {
    cellData = [];

    mapping[flatTableColumn].forEach((mappedArrayColumnID) => {
      if (ss.cells[mappedArrayColumnID + rowString] || ss.cells[mappedArrayColumnID + rowString] === 0 || flatTableColumn === 'colour') {
        cellData.push(ss.cells[mappedArrayColumnID + rowString])
      }
    });
    // cellData handled as SINGLE-VALUE 
  } else {
    cellData = ss.cells[mapping[flatTableColumn] + rowString];
  }
  return cellData;
}

const CleanseDetailString = (rawDetail) => {
  // ----------------------------------------------------       
  // This function simply cleanses the names of the bird details
  // extracted from a spreadsheet cell. We need to ensure that 
  // we remove any odd characters

  // Partner is a reserved word! Replace it wholesale
  if (rawDetail === 'partner') {
    return 'bbPartner';
  }

  // Remove hyphens and spaces, remove parenthesis and text between them, and force to all lower case.
  return rawDetail.split('-').join('').split(' ').join('').replace(/ *\([^)]*\) */g, "").toLowerCase();
}

const DetermineBandForm = (data) => {
  // ----------------------------------------------------     
  var form = null;

  // Return default type if NULL
  if (!data || typeof data === 'undefined') return form;

  // Convert to trimmed upper case string for string matching
  data = String(data).trim().toUpperCase();

  // Populate the form
  if (data === 'BUTT') { form = "BUTT"; }
  else if (data.includes('BUTT HALF METAL')) { form = "BUTT_HALF_METAL"; }
  else if (data.includes('1 WRAP, WRAPAROUND')) { form = "WRAPAROUND_1"; }
  else if (data.includes('1.5 WRAP, WRAPAROUND')) { form = "WRAPAROUND_1_5"; }
  else if (data.includes('1.75 WRAP, WRAPAROUND')) { form = "WRAPAROUND_1_75"; }
  else if (data.includes('2 WRAP, WRAPAROUND')) { form = "WRAPAROUND_2"; }
  else if (data.includes('UNSPECIFIED WRAPAROUND')) { form = "WRAPAROUND_UNKNOWN"; }
  else if (data.includes('SPIRAL')) { form = "SPIRAL"; }
  else if (data.includes('OTHER')) { form = "NONE"; }

  // Return the form
  return form;
}

const DetermineBandMaterial = (data) => {
  // ----------------------------------------------------     
  var material = null;

  // Return default type if NULL
  if (!data || typeof data === 'undefined') return material;

  // Convert to trimmed upper case string for string matching
  data = String(data).trim().toUpperCase();

  // Populate the material
  if (data.includes('METAL')) { material = "METAL"; }
  else if (data.includes('PLASTIC - UNSPECIFIED')) { material = "PLASTIC_UNSPECIFIED"; }
  else if (data.includes('ACETATE')) { material = "ACETATE"; }
  else if (data.includes('DARVIC')) { material = "DARVIC"; }
  else if (data.includes('ACRYLIC')) { material = "ACRYLIC"; }
  else if (data.includes('CELLULOID')) { material = "CELLULOID"; }
  else if (data.includes('OTHER')) { material = "OTHER"; }

  // Return the material
  return material;
}

const DetermineBandAttachmentType = (data) => {
  // ----------------------------------------------------     
  var attachmentType = null;

  // Return default type if NULL
  if (!data || typeof data === 'undefined') return attachmentType;

  // Convert to trimmed upper case string for string matching
  data = String(data).trim().toUpperCase();

  // Populate the attachment type
  if (data.includes('SUPER')) { attachmentType = "SUPERGLUE"; }
  else if (data.includes('GLUE')) { attachmentType = "GLUE"; }
  else if (data.includes('THF')) { attachmentType = "THF"; }
  else if (data.includes('TETRAHYDROFURAN')) { attachmentType = "THF"; }
  else if (data.includes('PLASTIC')) { attachmentType = "PLASTIC"; }
  else if (data.includes('SOLDER')) { attachmentType = "SOLDER"; }
  else if (data.includes('TAPE')) { attachmentType = "TAPE"; }
  else if (data.includes('CEMENT')) { attachmentType = "PIPE_CEMENT"; }
  else if (data.includes('NONE')) { attachmentType = "NONE"; }

  // Return the attachment type
  return attachmentType;
}

const DetermineBandColour = (data) => {
  // ----------------------------------------------------     
  var colour = null;

  // Return default type if NULL
  if (!data || typeof data === 'undefined') return colour;

  // Convert to trimmed upper case string for string matching
  data = String(data).trim().toUpperCase();

  switch (data) {
    case 'K': colour = 'BLACK'; break;
    case 'GR': colour = 'GREY'; break;
    case 'W': colour = 'WHITE'; break;
    case 'R': colour = 'RED'; break;
    case 'O': colour = 'ORANGE'; break;
    case 'PO': colour = 'PALE_ORANGE'; break;
    case 'NO': colour = 'FLUORESCENT_ORANGE'; break;
    case 'Y': colour = 'YELLOW'; break;
    case 'PP': colour = 'PALE_PINK'; break;
    case 'P': colour = 'PINK'; break;
    case 'NP': colour = 'FLUORESCENT_PINK'; break;
    case 'CP': colour = 'CRIMSON_PINK'; break;
    case 'LPU': colour = 'LIGHT_PURPLE'; break;
    case 'NPU': colour = 'FLUORESCENT_PURPLE'; break;
    case 'PU': colour = 'PURPLE'; break;
    case 'PB': colour = 'PALE_BLUE'; break;
    case 'NB': colour = 'FLUORESCENT_BLUE'; break;
    case 'LB': colour = 'LIGHT_BLUE'; break;
    case 'B': colour = 'BLUE'; break;
    case 'DB': colour = 'DARK_BLUE'; break;
    case 'NG': colour = 'FLUORESCENT_GREEN'; break;
    case 'LG': colour = 'LIME_GREEN'; break;
    case 'PG': colour = 'PALE_GREEN'; break;
    case 'G': colour = 'GREEN'; break;
    case 'DG': colour = 'DARK_GREEN'; break;
    case 'BR': colour = 'BROWN'; break;
    // Striped colour options
    case 'BSP': colour = 'BLUE_STRIPED_PINK'; break;
    case 'PSB': colour = 'PINK_STRIPED_BLUE'; break;
    case 'GSPU': colour = 'GREEN_STRIPED_PURPLE'; break;
    case 'PUSG': colour = 'PURPLE_STRIPED_GREEN'; break;
    case 'RSW': colour = 'RED_STRIPED_WHITE'; break;
    case 'WSR': colour = 'WHITE_STRIPED_RED'; break;
    case 'WSP': colour = 'WHITE_STRIPED_PINK'; break;
    case 'PSW': colour = 'PINK_STRIPED_WHITE'; break;
    case 'BSY': colour = 'BLUE_STRIPED_YELLOW'; break;
    case 'YSB': colour = 'YELLOW_STRIPED_BLUE'; break;
    case 'DGSO': colour = 'DARK_GREEN_STRIPED_ORANGE'; break;
    case 'WSPB': colour = 'WHITE_STRIPED_PALE_BLUE'; break;
    case 'YSO': colour = 'YELLOW_STRIPED_ORANGE'; break;
    case 'WSDG': colour = 'WHITE_STRIPED_DARK_GREEN'; break;
    case 'YSB': colour = 'YELLOW_STRIPED_BLUE'; break;
    case 'RSO': colour = 'RED_STRIPED_ORANGE'; break;
    case 'YSR': colour = 'YELLOW_STRIPED_RED'; break;
    case 'YSPB': colour = 'YELLOW_STRIPED_PALE_BLUE'; break;
    case 'OSDG': colour = 'ORANGE_STRIPED_DARK_GREEN'; break;
    case 'PBSW': colour = 'PALE_BLUE_STRIPED_WHITE'; break;
    case 'OSY': colour = 'ORANGE_STRIPED_YELLOW'; break;
    case 'DGSW': colour = 'DARK_GREEN_STRIPED_WHITE'; break;
    case 'OSR': colour = 'ORANGE_STRIPED_RED'; break;
    case 'RSY': colour = 'RED_STRIPED_YELLOW'; break;
    case 'PBSY': colour = 'PALE_BLUE_STRIPED_YELLOW'; break;
    default: colour = 'OTHER'; break;
  }

  // Return the colour
  return colour;
}

const DetermineMetalBandColour = (row, mapping) => {
  // ----------------------------------------------------    
  // If a metal band colour is provided, overwrite the default 'silver' colouring with that specified in the spreadsheet 
  var colour = (mapping['metalColour'] && row[mapping['metalColour']]) ? String(row[mapping['metalColour']]).trim().toUpperCase() : 'SILVER';
  return colour;
}

const DetermineMetalBandDetails = (row, mapping) => {
  // ----------------------------------------------------   
  var other = null;

  if (mapping['metalPosition'] && row[mapping['metalPosition']]) {
    var columnSpecifiedPosition = row[mapping['metalPosition']];

    // Determine position relative to colours  
    if (columnSpecifiedPosition > 6) { other = '[below colours/no colours]'; }
    else if (columnSpecifiedPosition > 4) { other = '[between colours]'; }
    else if (columnSpecifiedPosition > 2) { other = '[above colours]'; }
  }

  return other;
}

const DetermineFlagDetails = (row, mapping) => {
  // ----------------------------------------------------   
  var other = null;

  if (mapping['flagPosition'] && row[mapping['flagPosition']]) {
    var columnSpecifiedPosition = row[mapping['flagPosition']];

    // Determine position relative to colours
    if (columnSpecifiedPosition > 10) { other = 'web/foot'; }
    else if (columnSpecifiedPosition > 8) { other = 'flipper/wing'; }
    else if (columnSpecifiedPosition > 6) { other = '[below colours/no colours]'; }
    else if (columnSpecifiedPosition > 4) { other = '[between colours]'; }
    else if (columnSpecifiedPosition > 2) { other = '[above colours]'; }
  }

  return other;
}

const IsStockEvent = (event_type) => {
  // ----------------------------------------------------
  return ['LOST', 'PRACTICE', 'OTHER', 'TRANSFER', 'NEW_MARK'].includes(event_type)
}

const PopulateTrackedBand = (row) => {
  // ----------------------------------------------------    
  // New mark object with defaults
  var newMark = {
    'side': null,
    'position': null,
    'location_idx': 0,
    'mark_form': null,
    'mark_type': 'LEG_BAND',
    'mark_material': 'METAL',
    'mark_fixing': null,
    'colour': null,
    'text_colour': null,
    'alphanumeric_text': `${String(row.prefix_number).toLowerCase()}-${String(row.short_number).toLowerCase()}`,
    'other_description': null
  };

  return newMark;
}

const CheckPrimaryMarkType = (row) => {
  // ----------------------------------------------------       
  switch (String(row.prefix_number).toUpperCase()) {
    case 'PIT': {
      return 'TRANSPONDER';
    }
    case 'WEB': {
      return 'WEB'
    }
    default: {
      return 'LEG'
    }
  }
}

const GetBandType = (rawBand, location = null, row) => {
  // ----------------------------------------------------
  // HANDLE LEG MARKS
  if (location && location === 'LEG' && rawBand.includes('T')) { return 'LEG_TRANSPONDER'; }
  else if (location && location === 'LEG' && rawBand.includes('*') && String(row.prefix_number).toLowerCase().includes('pit')) { return 'LEG_TRANSPONDER'; }
  else if (location && location === 'LEG') { return 'LEG_BAND'; }
  // HANDLE OTHER MARKS
  // TODO...
}

const GetBandFormMaterialFixing = (rawBand, colourBandMetadata, row) => {
  // ----------------------------------------------------
  // ***************************************************
  //  MARK CONFIGURATION -> FORM, MATERIAL AND FIXING
  // ***************************************************
  let formMaterialFixing = {
    mark_form: null,
    mark_material: null,
    mark_fixing: null
  };
  // ------------------
  // METAL BANDS
  // ------------------
  if (rawBand.includes('M')) {
    formMaterialFixing.mark_material = 'METAL';
  }
  // ------------------
  // LEG TRANSPONDERS
  // ------------------
  else if (rawBand.includes('T')) {
    // At present - do nothing
  }
  // ------------------
  // FLAGS
  // ------------------
  else if (rawBand.includes('F')) {
    formMaterialFixing.mark_form = 'FLAG';
    formMaterialFixing.mark_material = colourBandMetadata.material; // this is an implicit assumption approved by the business
    formMaterialFixing.mark_fixing = colourBandMetadata.fixing; // this is an implicit assumption approved by the business
  }
  // ------------------
  // PRIMARY MARK
  // ------------------
  else if (rawBand.includes('*')) {
    switch (CheckPrimaryMarkType(row)) {
      case 'LEG': {
        formMaterialFixing.mark_material = 'METAL';
        break;
      }
      case 'TRANSPONDER': {
        // At present - do nothing
        break;
      }
      default: {
        // Return a blank object to represent that mark configuration form/material/fixing
        // as these could not be interpreted reliably
        formMaterialFixing = {};
        break;
      }
    }
  }
  // ------------------
  // ALL OTHER BANDS
  // ------------------
  else {
    formMaterialFixing.mark_form = colourBandMetadata.form; // this is an implicit assumption approved by the business
    formMaterialFixing.mark_material = colourBandMetadata.material; // this is an implicit assumption approved by the business
    formMaterialFixing.mark_fixing = colourBandMetadata.fixing; // this is an implicit assumption approved by the business
  }

  return formMaterialFixing;
}

const GetBandColourAndText = (rawBand, markConfigUncertainty, row) => {
  // ----------------------------------------------------
  // ******************************************************************
  //  MARK CONFIGURATION -> COLOUR, TEXT_COLOUR AND ALPHANUMERIC_TEXT
  // ******************************************************************
  let colourTextColourAlphanumeric = {
    colour: null,
    text_colour: null,
    alphanumeric_text: null,
  }
  // Process band colours based on Modified NZNBBS Coding Scheme
  // -> This was developed for version 10 of the spreadsheet
  // -> Note: had to change Fluorescent Pink code, FP, to BP (i.e. 'Bright Pink')
  // ------------------
  // PRIMARY MARK
  // ------------------
  if (rawBand.includes('*')) {
    // Regex isolates different portions of each individual code 
    // Index[1] -> flag/metal/transponder/primary
    // Index[2] -> Colour Symbol
    // Index[3] -> Alphanumeric Text
    // Index[4] -> Text Colour Symbol

    var rawBandComponents = rawBand.match(/^([*]){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}(?:\(([a-zA-Z0-9\-]+)\)){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}$/);
    let parsedBandComponents = rawBandComponents.map(x => { return (typeof x !== 'undefined') ? String(x).toUpperCase() : undefined })
    colourTextColourAlphanumeric.colour = parsedBandComponents[2] ? DetermineBandColour(parsedBandComponents[2]) : null;
    colourTextColourAlphanumeric.text_colour = parsedBandComponents[4] ? DetermineBandColour(parsedBandComponents[4]) : null;
    colourTextColourAlphanumeric.alphanumeric_text = (row.prefix_number && row.short_number) ? `${String(row.prefix_number).toLowerCase()}-${String(row.short_number).toLowerCase()}` : null;
  }
  // -------------------------
  // BANDS WITH VALID CODING
  // -------------------------
  else if (rawBand.match(/^([FMT]){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}(?:\(([a-zA-Z0-9\-]+)\)){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}$/)) {
    // Regex isolates different portions of each individual code 
    // Index[1] -> flag/metal/transponder/primary
    // Index[2] -> Colour Symbol
    // Index[3] -> Alphanumeric Text
    // Index[4] -> Text Colour Symbol
    var rawBandComponents = rawBand.match(/^([FMT]){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}(?:\(([a-zA-Z0-9\-]+)\)){0,1}([BCDGKNOLPRSUWY]{1,4}){0,1}$/);
    let parsedBandComponents = rawBandComponents.map(x => { return (typeof x !== 'undefined') ? String(x).toUpperCase() : undefined })
    colourTextColourAlphanumeric.colour = parsedBandComponents[2] ? DetermineBandColour(parsedBandComponents[2]) : null;
    colourTextColourAlphanumeric.text_colour = parsedBandComponents[4] ? DetermineBandColour(parsedBandComponents[4]) : null;
    // Use the raw band components for the alphanumeric text (this will preserve case sensitivity)
    colourTextColourAlphanumeric.alphanumeric_text = (!markConfigUncertainty.alphanumeric && rawBandComponents[3]) ? String(rawBandComponents[3]).toLowerCase() : null;
  }
  // ---------------------------
  // BANDS WITH INVALID CODING
  // ---------------------------
  else {
    colourTextColourAlphanumeric = {
      colour: 'UNPARSEABLE',
      text_colour: null,
      alphanumeric_text: null,
    }
  }
  return colourTextColourAlphanumeric;
}

const ProcessRawLegBandArray = (rawBands, markConfigUncertainty, row) => {
  // ----------------------------------------------------       

  // Results array
  var marks = [];

  // Return empty array if NULL
  if (!rawBands) {
    return marks;
  }

  // ---------------------
  // Colour Band Metadata
  // ---------------------
  let colourBandMetadata = {
    material: ('mark_config_material' in row && row.mark_config_material) ? DetermineBandMaterial(row.mark_config_material) : null,
    form: ('mark_config_form' in row && row.mark_config_form) ? DetermineBandForm(row.mark_config_form) : null,
    fixing: ('mark_config_fixing' in row && row.mark_config_fixing) ? DetermineBandAttachmentType(row.mark_config_fixing) : null,
  };

  // Iterate over each raw band
  for (var i = 0; i < rawBands.length; i++) {

    // Grab the raw band
    var rawBand = String(rawBands[i].trim().toUpperCase());

    // Only process the band if we've got the minimum information to do so.
    if (rawBand.length) {

      // Initialise newMarkObject
      var newMark = {};

      // Bands included in these columns are specified as being on a birds leg
      // -> Update the mark_type,
      // -> Form/material/fixing, and,
      // -> Colour/TextColour/AlphanumericText
      newMark = {
        mark_type: GetBandType(rawBand, 'LEG', row),
        ...GetBandFormMaterialFixing(rawBand, colourBandMetadata, row),
        ...GetBandColourAndText(rawBand, markConfigUncertainty, row)
      }

      // If rawBand is a single or double word string,
      // Assume this is a string representing a single colour 
      // if (rawBand.match(/^[A-z]{2,}$|^[A-z]{2,}[\s.-][A-z]{2,}$/)) { 
      //   newMark.colour = rawBand.trim().toUpperCase();
      // }
      // // If rawBand is a 'M',
      // // Assume this is a metal band and assign the default colour
      // else if (rawBand === 'M') {
      //   newMark.type_tags = ['METAL']; 
      //   newMark.colour = DetermineMetalBandColour(row, mapping);
      //   newMark.other_description = DetermineMetalBandDetails(row, mapping); 
      //   newMark.attachment_type = 'NONE'; 
      // }
      // // If rawBand is a 'F',
      // // Assume this is a flag with an unknown colour 
      // else if (rawBand.trim().toUpperCase() === 'F') { 
      //   newMark.type_tags = ['FLAG'];
      //   newMark.other_description = DetermineFlagDetails(row, mapping);
      // }
      // // If rawBand follows the pattern 'F COLOUR or COLOUR F'
      // // Assume this is a non-alphanumeric coloured flag 
      // else if (rawBand.match(/^F[\s.-](.+)$/)) { 
      //   var rawBandComponents = rawBand.match(/^F[\s.-](.+)$/); 
      //   rawBandComponents = rawBandComponents.map(x => { return String(x).trim().toUpperCase() })
      //   newMark.type_tags = ['FLAG'];
      //   newMark.colour = String(rawBandComponents[1]).trim().toUpperCase();
      //   newMark.other_description = DetermineFlagDetails(row, mapping);         
      // } 
      // // If rawBand follows the pattern 'COLOUR text on [F?] COLOUR
      // // Aassume this is a non-alphanumeric flag with text and band colours 
      // else if (rawBand.match(/^(^[A-z]{2,}|^[A-z]{2,}[\s.-][A-z]{2,})[\s.-]text[\s.-]on[\s.-]([A-z]{2,}$|[A-z]{2,}[\s.-][A-z]{2,}$)|(^[A-z]{2,}|^[A-z]{2,}[\s.-][A-z]{2,})[\s.-]text[\s.-]on[\s.-]F[\s.-]([A-z]{2,}$|[A-z]{2,}[\s.-][A-z]{2,}$)$/)) { 
      //   var rawBandComponents = rawBand.match(/^(^[A-z]{2,}|^[A-z]{2,}[\s.-][A-z]{2,})[\s.-]text[\s.-]on[\s.-]([A-z]{2,}$|[A-z]{2,}[\s.-][A-z]{2,}$)|(^[A-z]{2,}|^[A-z]{2,}[\s.-][A-z]{2,})[\s.-]text[\s.-]on[\s.-]F[\s.-]([A-z]{2,}$|[A-z]{2,}[\s.-][A-z]{2,}$)$/); 
      //   rawBandComponents = rawBandComponents.map(x => { return String(x).trim().toUpperCase() })

      //   // This is therefore alphanumeric but need to assess whether this is a flag or not 
      //   newMark.colour = rawBandComponents[2] ? String(rawBandComponents[2]).trim().toUpperCase() : String(rawBandComponents[4]).trim().toUpperCase(); 
      //   newMark.text_colour = rawBandComponents[1] ? String(rawBandComponents[1]).trim().toUpperCase() : String(rawBandComponents[3]).trim().toUpperCase(); 
      //   newMark.alphanumeric_text = row[mapping['alphaNumericText']] ? row[mapping['alphaNumericText']] : null; 

      //   if (!(rawBandComponents[1] && rawBandComponents[2])){
      //     newMark.type_tags = ['FLAG'];
      //     newMark.other_description = DetermineFlagDetails(row, mapping);
      //   }
      // }
      // // Match NZNBBS mark code
      // else if(rawBand.match(/^(F){0,1}([WKRYBPBGDGPGLOPFPCPPUM]{1,2})(?:\(([a-zA-Z0-9]+)\))*$/)){
      //   // Regex isolates different portions of each individual code 
      //   // Index[1] -> flag 
      //   // Index[2] -> Colour Symbol 
      //   // Index[3] -> Alphanumeric Text
      //   var rawBandComponents = rawBand.match(/^(F){0,1}([WKRYBPBGDGPGLOPFPCPPUM]{1,2})(?:\(([a-zA-Z0-9]+)\))*$/);
      //   rawBandComponents = rawBandComponents.map(x => { return String(x).toUpperCase() })

      //   // Process metal bands separate from colour bands 
      //   if (rawBandComponents[2] === 'M') { 
      //     newMark.type_tags = ['METAL'];           
      //     newMark.colour = DetermineMetalBandColour(row, mapping);
      //     newMark.other_description = DetermineMetalBandDetails(row, mapping); 
      //     newMark.attachment_type = 'NONE';
      //   } 
      //   else { 
      //     newMark.type_tags = rawBandComponents[1] ? ['FLAG'] : newMark.type_tags; 
      //     newMark.colour = rawBandComponents[2] ? DetermineBandColour(rawBandComponents[2]) : null; 
      //     newMark.alphanumeric_text = rawBandComponents[3] ? rawBandComponents[3] : null; 
      //   } 
      // }

      // Push the populated mark onto the array
      marks.push(newMark);
    }
  }

  return marks;
}

const parsePersonDetails = (rowValue, flatTableColumn) => {
  // ----------------------------------------------------       
  // console.debug("bb_spreadsheet_helpers.parsePersonDetails())"); 
  var personProperties = [];

  var personProperty = {
    personType: null,
    propertyName: null,
    value: null
  };

  // Determine whether this is an L3 / Bander / Other person
  personProperty.personType = flatTableColumn.match(/(?<=proposed).*((?=Number)|(?=Name)|(?=OrgName)|(?=Contact))/g)[0];
  // Determine whether this is Number / Name / OrgName / Contact (camel case)
  personProperty.propertyName = Helpers.camelize(flatTableColumn.match(new RegExp('(?<=proposed' + personProperty.personType + ').*', 'g'))[0]);
  // NOTE -> We need personType case sensitivity to this point for use in deriving the property name! Don't be tempted to move the line below this without broader refactoring :) 
  personProperty.personType = personProperty.personType.toLowerCase();

  // If the personProp is 'number' we need to pad the answer with zeros to 4 digits! 
  // The assumption is that beyond 4 digits (assuming this will happen in future), that the number will no longer be padded 
  if (personProperty.propertyName === 'number') {
    personProperty.value = rowValue ? Helpers.padNumeric(rowValue, 4, '0') : null;
    personProperties.push(personProperty);
  }
  // IF the personProp is 'name' there's a whole heap of cleansing and pre-processing we need to perform.
  // We might need to extract a whole heap of sub values, or split based on initials or first name, last name
  // or any of a host of other issues.
  else if (rowValue && personProperty.propertyName === 'name') {
    var personProperty2 = cloneDeep(personProperty);
    personProperty.propertyName = 'firstName';
    personProperty2.propertyName = 'surname';

    // Check if any sub-parameters are included inside the name field 
    if (rowValue.includes('org:') || rowValue.includes('person:')) {
      var parseSpecialPersonInput = rowValue.match(/(?:(?:^person:)(.*)(?:::)(?:(?:org:)(.*)$))|(?:(?:^person:)(.*)$)|(?:(?:^org:)(.*)$)/);
      // If first two capture groups returned, we have an orgName and a person to process 
      if (parseSpecialPersonInput[1] && parseSpecialPersonInput[2]) {
        var personProperty3 = cloneDeep(personProperty);
        personProperty3.propertyName = 'orgName';
        var nameSplit = parseSpecialPersonInput[1].split(/[\s,.]+/).filter(word => word.length > 0);
        personProperty.value = nameSplit.slice(0, -1).join(' ');
        personProperty2.value = nameSplit.pop();
        personProperty3.value = parseSpecialPersonInput[2];
        personProperties.push(personProperty, personProperty2, personProperty3);
      }
      // If third capture group returned, we have a person to process ONLY 
      else if (parseSpecialPersonInput[3]) {
        var nameSplit = parseSpecialPersonInput[3].split(/[\s,.]+/).filter(word => word.length > 0);
        personProperty.value = nameSplit.slice(0, -1).join(' ');
        personProperty2.value = nameSplit.pop();
        personProperties.push(personProperty, personProperty2);
      }
      // If fourth (and final) capture group is returned, we have a orgName to process ONLY -> default to unknown person! 
      else if (parseSpecialPersonInput[4]) {
        var personProperty3 = cloneDeep(personProperty);
        personProperty3.propertyName = 'orgName';
        personProperty.value = 'Unknown';
        personProperty2.value = 'Unknown';
        personProperty3.value = parseSpecialPersonInput[4];
        personProperties.push(personProperty, personProperty2, personProperty3);
      }
    }
    // No sub-parameters in the field. Just try to parse out the name.
    else {
      var nameSplit = rowValue.split(/[\s,.]+/).filter(word => word.length > 0);
      personProperty.value = nameSplit.slice(0, -1).join(' ');
      personProperty2.value = nameSplit.pop();
      personProperties.push(personProperty, personProperty2);
    }
  }
  // Handle Org Name or Contact. Just take the value as-is.
  else {
    personProperty.value = rowValue ? String(rowValue) : null;
    personProperty.value && personProperties.push(personProperty);
  }
  return personProperties;
}

const TransformLocationComment = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case '1. northland': result = 'NORTHLAND'; break;
    case '2. auckland': result = 'AUCKLAND'; break;
    case '3. waikato': result = 'WAIKATO'; break;
    case '4. bay of plenty': result = 'BAY OF PLENTY'; break;
    case '5. gisborne': result = 'GISBORNE'; break;
    case '6. hawke\'s bay' : result = 'HAWKE\'S BAY'; break;
    case '7. taranaki': result = 'TARANAKI'; break;
    case '8. manawatu-whanganui': result = 'MANAWATU-WHANGANUI'; break;
    case '9. wellington': result = 'WELLINGTON'; break;
    case '10. west coast': result = 'WEST COAST'; break;
    case '11. canterbury': result = 'CANTERBURY'; break;
    case '12. otago': result = 'OTAGO'; break;
    case '13. southland': result = 'SOUTHLAND'; break;
    case '14. tasman': result = 'TASMAN'; break;
    case '15. nelson': result = 'NELSON'; break;
    case '16. marlborough': result = 'MARLBOROUGH'; break;
    case '17. chathams': result = 'CHATHAMS'; break;
    case '18. subantarctic islands': result = 'SUBANTARCTIC ISLANDS'; break;
    case '19. pacific islands': result = 'PACIFIC ISLANDS'; break;
    case '20. at sea': result = 'AT SEA'; break;
    case '21. australia': result = 'AUSTRALIA'; break;
    case '22. other': result = 'OTHER'; break;
    default: result = null;
  }

  // Return the result
  return result;
}

const TransformBirdRegionCode = (rowData, gamebird) => {
  // ----------------------------------------------------
  // If no data, return immediately.
  if (!rowData)
    return null;

  return (gamebird ? 'GBR' : 'BBR') + parseRegex(String(rowData).toLowerCase(), /^([0-9]{1,2})(?:[. ]{1,2}(?:northland|auckland|waikato|bay of plenty|gisbourne|hawke's bay|taranaki|manawatu-whanganui|wellington|west coast|canterbury|otago|southland|tasman|nelson|marlborough|chathams|subantarctic islands|pacific islands|at sea|australia|other){1}.*){0,1}$/);
}

const parseRegex = (cellData, regex) => {
  // ----------------------------------------------------       
  // console.debug("bb_spreadsheet_helpers.parseRegex())"); 
  var parsedProperty = String(cellData).toLowerCase().match(regex);
  return (parsedProperty && parsedProperty.length > 1) ? parsedProperty[1] : cellData;
}

const TransformEventType = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case 'first-mark-in-hand': result = 'FIRST_MARKING_IN_HAND'; break;
    case 'in-hand': result = 'IN_HAND'; break;
    case 'in-hand-pre-change': result = 'IN_HAND_PRE_CHANGE'; break;
    case 'in-hand-post-change': result = 'IN_HAND_POST_CHANGE'; break;
    case 'sighted-by-person': result = 'SIGHTING_BY_PERSON'; break;
    case 'recorded-by-technology': result = 'RECORDED_BY_TECHNOLOGY'; break;
    case '51. band lost or used for training': result = 'LOST'; break;
    case '53. band issued for practice': result = 'PRACTICE'; break;
    case '54. band for other use': result = 'OTHER'; break;
    default: result = null;
  }

  // Return the result
  return result;
}

const TransformMarkState = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case 'first_marking_in_hand':
    case 'in_hand':
    case 'in_hand_pre_change':
    case 'sighting_by_person':
    case 'recorded_by_technology':
    case 'in_hand_post_change':
      result = 'ATTACHED'; 
      break;
    case 'lost': result = 'LOST'; break;
    case 'practice': result = 'PRACTICE'; break;
    case 'other': result = 'OTHER'; break;
    default: result = null; break;
  }

  // Return the result
  return result;
}

const TransformBandingSchemeCode = (value) => {
  // ----------------------------------------------------
  // Translate the values in order of increasing specificity,
  // defaulting to NZ gamebird
  var rawValue = value.trim().toLowerCase();
  if (rawValue.includes("foreign")) return "FOREIGN";
  if (rawValue.includes("non-gamebird")) return "NZ_NON_GAMEBIRD";
  return "NZ_GAMEBIRD";
}

const TransformDateTime = (value, accuracyModifier = null) => {
  // ----------------------------------------------------
  // tries to parse the icnoming datetime value from either a string
  // or a number value into a standard ISO string, optionally
  // modifying the value for accuracy.

  const CUSTOM_EXCEL_DATE_STRING_FORMAT_1 = 'MM/DD/YY HH:mm';
  const CUSTOM_EXCEL_DATE_STRING_FORMAT_2 = 'DD MMMM YYYY';

  var typeOfValue = typeof value;
  var dateTimeRaw = value;
  var dateTimeString;
  var accuracyMod = accuracyModifier ? accuracyModifier.trim().toUpperCase() : null;

  // Handle string type. Assume ISO8601 format
  if (typeOfValue === 'string') {
    if (Moment(dateTimeRaw, [CUSTOM_EXCEL_DATE_STRING_FORMAT_1, CUSTOM_EXCEL_DATE_STRING_FORMAT_2, Moment.ISO_8601]).isValid()) {
      var m = MomentTimezone.tz(dateTimeRaw, [CUSTOM_EXCEL_DATE_STRING_FORMAT_1, CUSTOM_EXCEL_DATE_STRING_FORMAT_2, MomentTimezone.ISO_8601], "NZ");
      switch (accuracyMod) {
        case 'D': m = m.startOf('day'); break;
        case 'M': m = m.startOf('month'); break;
        case 'Y': m = m.startOf('year'); break;
      }
      dateTimeString = m.toDate().toISOString();
    }
    else {
      dateTimeString = dateTimeRaw;
    }
  }
  // Handle number type. Could be weird Excel days format, or normal Unix timestamp.
  // Assume the former, given we're parsing spreadsheets
  else if (typeOfValue === 'number') {
    var m = MomentTimezone.tz(Helpers.excelDateToDate(dateTimeRaw), "NZ");
    switch (accuracyMod) {
      case 'D': m = m.startOf('day'); break;
      case 'M': m = m.startOf('month'); break;
      case 'Y': m = m.startOf('year'); break;
    }
    dateTimeString = m.toDate().toISOString();
  }

  // Return the string version of the date-time
  return dateTimeString;
}

const TransformCaptureType = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var identifier = rawValue.substr(0, rawValue.indexOf('.'));
  var result = null;

  switch (identifier) {
    case '1': result = 'CAUGHT_BY_HAND'; break;
    case '1a': result = 'CAPTURED_IN_A_NATURAL_NEST'; break;
    case '1b': result = 'CAPTURED_IN_A_NEST_BOX'; break;
    case '2': result = 'CAUGHT_BY_HAND_NET'; break;
    case '3': result = 'CAUGHT_BY_CANNON_NET'; break;
    case '4': result = 'CAUGHT_IN_MIST_NET'; break;
    case '5': result = 'CAUGHT_IN_FISHING_NET'; break;
    case '6': result = 'CAUGHT_BY_NOOSE_MAT'; break;
    case '7': result = 'CAUGHT_BY_CLAP_TRAP'; break;
    case '8': result = 'CAUGHT_BY_WHOOSH_NET'; break;
    case '9': result = 'CAUGHT_BY_POTTER_TRAP'; break;
    case '10': result = 'CAUGHT_BY_PULL_ACTIVATED_DROP_TRAP'; break;
    case '11': result = 'CAUGHT_BY_FUNNEL_TRAP'; break;
    case '12': result = 'CAUGHT_IN_PULL_ACTIVATED_LEG_NOOSE'; break;
    case '13': result = 'CAUGHT_BY_NECK_HOOP_OR_CROOK'; break;
    case '14': result = 'CAUGHT_BY_NOOSE_HAT_OR_BAL_CHATRI'; break;
    case '15': result = 'CAUGHT_BY_NET_GUN'; break;
    case '16': result = 'CAUGHT_BY_CAGE_TRAP'; break;
    case '17': result = 'CAPTURED_AT_FEEDER'; break;
    default: result = 'OTHER';
  }

  // Return the result
  return result;
}

const TransformEventSituation = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case 'wild': result = 'WILD'; break;
    case 'source site': result = 'SOURCE_SITE'; break;
    case 'captivity': result = 'CAPTIVE'; break;
    case 'release site': result = 'RELEASE_SITE'; break;
    default: result = null;
  }

  // Return the result
  return result;
}

const TransformStatusCode = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case 'dead: unspecified': result = 'DEAD_UNSPECIFIED'; break;
    case 'dead: recent': result = 'DEAD_RECENT'; break;
    case 'dead: not recent': result = 'DEAD_NOT_RECENT'; break;
    case 'unknown: bandonly': result = 'UNKNOWN_BAND_ONLY'; break;
    case 'alive': result = 'ALIVE'; break;
    default: result = 'UNKNOWN';
  }

  // Return the result
  return result;
}

const TransformConditionCode = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case '0. good': result = 'GOOD'; break;
    case '1. poor': result = 'POOR'; break;
    case '2. injured / sick': result = 'INJURED_SICK'; break;
    case '3. rehabilitated': result = 'REHABILITATED'; break;
    case '4. artificially reared': result = 'ARTIFICIALLY_REARED'; break;
    case '99. other: add in notes': result = 'OTHER'; break;
    default: result = 'UNKNOWN';
  }

  // Return the result
  return result;
}

const TransformOtherMarkType = (value) => {
  // ----------------------------------------------------
  // Translate the values
  var rawValue = value.trim().toLowerCase();
  var result = null;

  switch (rawValue) {
    case 'web tag': result = 'WEB'; break;
    case 'insertion transponder': result = 'INSERTED_TRANSPONDER'; break;
    case 'flipper band': result = 'FLIPPER'; break;
    case 'jess': result = 'JESS'; break;
    case 'gps': result = 'GPS'; break;
    case 'tdr': result = 'TDR'; break;
    case 'gls': result = 'GLS'; break;
    case 'vhf': result = 'TRANSMITTER'; break;
    default: result = 'OTHER';
  }

  // Return the result
  return result;
}

const JoinIdsToMarks = (markConfiguration, lookupData) => {
  // ----------------------------------------------------
  return markConfiguration.reduce((arrayBuilder, individualMark) => {
    // Check whether the mark has alphanumeric text that is consistent with a managed stock prefix_number/short_number
    let markToAdd = Object.assign({}, individualMark);
    let markSplit = (typeof individualMark.alphanumeric_text !== 'undefined' && individualMark.alphanumeric_text) ? individualMark.alphanumeric_text.split('-') : [];

    // Join ID only if an ID hasn't already been assigned (e.g. from the frontend via JSON)
    if ((typeof markToAdd.mark_id === 'undefined' || !markToAdd.mark_id) && individualMark.mark_material === 'METAL' && markSplit.length === 2) {
      let rawPrefixNumber = markSplit[0];
      let rawShortNumber = markSplit[1];
      // For these cases - get the mark.id from the lookup data
      var mark = lookupData.marks.find(obj => (obj.prefix_number === rawPrefixNumber && obj.short_number === rawShortNumber));
      // ----------------------------------
      if (typeof mark === 'undefined' || !mark) {
        console.info(`Mark not found ${rawPrefixNumber}-${rawShortNumber}!`);
      }
      else {
        markToAdd.mark_id = mark.id;
      }
    }
    arrayBuilder.push(markToAdd);
    return arrayBuilder;
  }, []);
}

// Function to upload an event to the database and return 
const UploadIndividualEvent = (db, event, index = null, eventUploadRecords = { events: [], eventBatchBirdIdMarkRelations: {} }) => {
  // ----------------------------------------------------------------------------
  console.info("BBBusinessValidationAndErrors.BirdEventValidation()");
  console.info(`Uploading event ${JSON.stringify(event)}`);

  return new Promise((resolve, reject) => {
    return db.withTransaction(async tx => {
      // --------------------------------------------------------------------
      // If this is a bird event, handle the bird component of the event
      let birdInsert = null;
      let eventMarksArray = [];
      let birdMarkMatches = [];
      
      // ---------------------------
      // Bird Upload  (only for bird events - complex logic required to determine if new bird needs to be created)
      // Case 1) If a single existing bird is found, associate this event with that bird
      // Case 2) If either, 0 existing birds, or more than 1 existing bird is found, 
      //      -> create a new bird because we cannot reliably create an association 
      if (IsBirdEvent(event.event.event_type)
        && (typeof event.bird.id === 'undefined'
          || !event.bird.id)) {
        // -------------------
        // Check if any previously added birds have the same marks, if so, assign the same bird_id to these
        //  Get list of tracked marks for this event
        eventMarksArray = event.mark_state.reduce((arrayBuilder, markState) => {
          if (typeof markState.mark_id !== 'undefined' && markState.mark_id) {
            arrayBuilder.push(markState.mark_id);
          }
          return arrayBuilder;
        }, []);

        //  Match birds based on previous uploads for this batch if there is a corresponding mark_id 
        birdMarkMatches = Object.keys(eventUploadRecords.eventBatchBirdIdMarkRelations)
          .filter(markBirdLookup => {
            return eventMarksArray.includes(markBirdLookup);
          })
          .map(matchedMark => eventUploadRecords.eventBatchBirdIdMarkRelations[matchedMark]);

        if (birdMarkMatches.length === 1) {
          // ----------------------------------
          // CASE 1
          console.info('Bird found, creating association: ', birdMarkMatches[0]);
          birdInsert = await tx.bird.save({
            id: birdMarkMatches[0],
            species_id: event.bird.species_id
          });
        }
        else {
          // ----------------------------------
          // CASE 2
          // Insert a new bird because no ID has been assigned
          console.info('Creating a new bird: ', birdMarkMatches[0]);
          birdInsert = await tx.bird.save({
            species_id: event.bird.species_id
          });
        }
      }

      // Check if new bird has been created and insert the bird_id otherwise, bird_id should have been added previously for a bird record
      if (birdInsert) {
        event.bird = birdInsert;
        event.event.bird_id = birdInsert.id;
        // If there haven't been matches to previous records, add the new bird id /mark combinations to the eventBatchBirdIdRelations
        eventMarksArray.map(markId => {  if (birdMarkMatches.length > 0) { return } else { eventUploadRecords.eventBatchBirdIdMarkRelations[markId] = event.bird.id; return; } });
      }

      // ---------------------------
      // Event Upload (required in all cases...)
      const eventInsert = await tx.event.save(event.event);
      event.event = eventInsert;

      // ---------------------------
      // Characteristic Measurement Upload
      // Either:
      // 1) an array of characteristics, or
      // 2) an empty array (e.g stock records)
      event.characteristic_measurements = event.characteristic_measurements.map(characteristicMeasurement => {
        return { ...characteristicMeasurement, 'event_id': eventInsert.id };
      });
      const characeristicInsert = await tx.characteristic_measurement.insert(event.characteristic_measurements);
      event.characteristic_measurements = characeristicInsert;

      // ---------------------------
      // Mark Configuration Upload
      // Either:
      // 1) an array of mark configurations, or
      // 2) an empty array (e.g stock records)
      event.mark_configuration = event.mark_configuration.map(markConfiguration => {
        return { ...markConfiguration, 'event_id': eventInsert.id };
      });
      const markConfigurationInsert = await tx.mark_configuration.insert(event.mark_configuration);
      event.mark_configuration = markConfigurationInsert;

      // ---------------------------
      // Mark State Upload (required in all cases...)
      event.mark_state = event.mark_state.map(markState => {
        return { ...markState, 'event_id': eventInsert.id };
      });
      const markStateInsert = await tx.mark_state.insert(event.mark_state);
      event.mark_state = markStateInsert;

      // ----------------------------
      // Mark Allocation
      // Either:
      // 1) an array of mark allocations, or
      // 2) an empty array (e.g BIRD records)
      event.mark_allocation = event.mark_allocation.map(markAllocation => {
        return { ...markAllocation, 'event_id': eventInsert.id };
      });
      const markAllocationInsert = await tx.mark_allocation.insert(event.mark_allocation);
      event.mark_allocation = markAllocationInsert;

      eventUploadRecords.events.push(event);

      return resolve(eventUploadRecords);
    })
  })
}

module.exports = {
  IsBirdEvent,
  IsTransferEvent,
  IsInHandEvent,
  PreProcessMarkConfigurations,
  CheckPrimaryMarkType,
  processCellData,
  CleanseDetailString,
  IsStockEvent,
  PopulateTrackedBand,
  ProcessRawLegBandArray,
  parsePersonDetails,
  TransformBirdRegionCode,
  TransformLocationComment,
  parseRegex,
  TransformEventType,
  TransformMarkState,
  TransformBandingSchemeCode,
  TransformDateTime,
  TransformCaptureType,
  TransformEventSituation,
  TransformStatusCode,
  TransformConditionCode,
  TransformOtherMarkType,
  JoinIdsToMarks,
  UploadIndividualEvent,
  GetCodeFromRegion,
  GetRegionFromCode
};