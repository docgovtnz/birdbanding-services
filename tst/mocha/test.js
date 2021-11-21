let assert = require('assert');
const BBSSHelpers = require('./bb-spreadsheet-helpers');

// Test variables
const test_rows = require('./reference_data').row;
const against_rows = require('./reference_data').row_against;

// CONSTANTS

// Internal functions to serverless project to TEST
const populateMarkConfigurationFromRow = (row) => {
  // ---------------------------------------------------------------------------- 
  try {
    // **** REFACTOR ****
    // With next method
    var marks = [];
    var bandArray = [];
    var bandData = null;
    var rawBands = [];
    var mark_config_components = [
      'mark_config_left_tibia', 'mark_config_left_tarsus',
      'mark_config_right_tibia', 'mark_config_right_tarsus',
      'out_mark_config_other_mark_type'
    ];

    // -------------
    // Uncertainty
    // -------------
    let markConfigUncertainty = {};
    if ('mark_config_uncertainty' in row && row.mark_config_uncertainty) {
      markConfigUncertainty = {
        side: (row.mark_config_uncertainty.toLowerCase().includes('side')),
        position: (row.mark_config_uncertainty.toLowerCase().includes('position')),
        alphanumeric: (row.mark_config_uncertainty.toLowerCase().includes('alphanumeric')),
        location_idx: (row.mark_config_uncertainty.toLowerCase().includes('location')),
      };
    } else {
      markConfigUncertainty = {
        side: false,
        position: false,
        alphanumeric: false,
        location_idx: false
      }
    }

    mark_config_components.forEach(component => {
      // Has the mark_config component been filtered out during preprocessing...?
      if (component in row) {
        // -----------------------
        bandData = row[component];
        if (bandData && bandData.includes('(enter bands on')) {
          return;
        }
        else if (component === 'out_mark_config_other_mark_type') {
          let otherMark = {
            "side": null,
            "position": null,
            "location_idx": null,
            "mark_type": BBSSHelpers.TransformOtherMarkType(bandData),
            "mark_form": null,
            "mark_material": null,
            "mark_fixing": null,
            "colour": null,
            "text_colour": null,
            "alphanumeric_text": ('mark_config_other_mark_alpha' in row && row.mark_config_other_mark_alpha) ? mark_config_other_mark_alpha : null
          }
          // Add the other mark to the mark_configuration array
          marks.push(otherMark);
        }
        rawBands = bandData ? String(bandData).split(/[,.;/]+/).map(word => word.trim()) : null;
        bandArray = BBSSHelpers.ProcessRawLegBandArray(rawBands, markConfigUncertainty, row);
        bandArray.forEach((band, idx) => {
          marks.push({
            'side': !markConfigUncertainty.side ? component.match(/(right|left)/g)[0].toUpperCase() : null,
            'position': !markConfigUncertainty.position ? component.match(/(tibia|tarsus)/g)[0].toUpperCase() : null,
            'location_idx': !markConfigUncertainty.location_idx ? idx : null,
            ...band
          });
        });
      }
    });

    // -----------------------------------------------------------------------
    // ENSURE PRIMARY MARK HAS BEEN ADDED TO THE MARK_CONFIGURATION
    // -----------------------------------------------------------------------
    // At this point, we've done all we can to determine the bands on the bird
    // from the available columns. The only thing left to process is the case
    // where no specific band data has been provided, and we've just (possibly)
    // got our tracked metal band. If that's the case, we need to wring out
    // more information related to that - from even more potential columns.
    // NOTE: this only applies if the event_type is not POST CHANGE
    //      reason being: in this case we need to know explicitly if the
    //      primary mark has been removed or not... therefore we can't implicitly add it
    if (BBSSHelpers.TransformEventType(row.event_type) !== 'IN_HAND_POST_CHANGE' && marks.length <= 0 && row.short_number && row.prefix_number && !['PIT', 'WEB'].includes(row.prefix_number.toUpperCase())) {
      marks.push(BBSSHelpers.PopulateTrackedBand(row));
    }

    // Return the list of populated marks
    return marks;
  }
  catch (err) {
    console.error(err);
    throw new Error(err);
  }
};

// Complex feature -> parsing mark configuration codes
describe('Test raw band parsing', () => {
  describe('Raw Band Array Test Cases', () => {
    it('Should be parsed to the expected marks array', () => {

      // For each test case, parse the rawBands and generated the colour bands array
      let markConfigurations = test_rows.map(row => {
        return populateMarkConfigurationFromRow(row);
      });

      markConfigurations.map((markConfig, idx) => {
        assert.deepStrictEqual(markConfig, against_rows[idx]);
      })
    });
  });
});
