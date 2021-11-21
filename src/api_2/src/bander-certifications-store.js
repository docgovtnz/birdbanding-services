class BanderCertifications
{
constructor(json, db) {
  console.debug("__________Certification payload_____________")
  console.debug(JSON.stringify(json));
  console.debug("____________________________________________")
  this.json = Object.assign({}, json);
  this.db = db;
  this.errors = [];
  this.notFound = false;
  this.newRecord;
}

save() {
  const PG_UNIQUENESS_ERROR = '23505';
  const PG_FOREIGN_KEY_ERROR = '23503';
  this.json = BanderCertifications.convertCertToDBFields(this.json);
  return this.db.bander_certifications.save(
    this.json,
    BanderCertifications.returnFields
  ).then(result => {
    if (!result) {
      this.notFound = true;
      return false;
    }
    result = BanderCertifications.convertDBFieldsToCert(result);
    this.newRecord = result;
    return true;
  })
    .catch(e => {
      console.debug("ERROR");
      console.debug(JSON.stringify(e));
      if(e.code && e.code == PG_UNIQUENESS_ERROR){
        this.errors.push("A record for this bander and endorsement/species group already exists");
      } 
      else if(e.code && e.code == PG_FOREIGN_KEY_ERROR){
        this.errors.push("That species group id or bander id doesn't exist");
        }
        else{
          this.errors.push("Error saving");
        }
        console.info("Save error");
        console.debug(e);
        return false;
      });
  }

  
  //in db certification field = certification|endorsement. These convert:

  static isSpeciesGroup(bcRec) {
    const potential_sg_id = parseInt(bcRec.certification);
    return potential_sg_id == bcRec.certification;
  }

  static convertCertToDBFields(bcRec) {
    if (BanderCertifications.isSpeciesGroup(bcRec)) {
      bcRec['species_group_id'] = parseInt(bcRec.certification);
      bcRec['endorsement'] = null;
    }
    else {
      bcRec['endorsement'] = bcRec.certification;
      bcRec['species_group_id'] = null;
    }
    delete bcRec.certification;
    return bcRec;
  }

  static convertDBFieldsToCert(bcRec) {
    bcRec.certification = bcRec['species_group_id'] || bcRec['endorsement'];
    bcRec.certification_type = bcRec['species_group_id'] ? 'SPECIES_GROUP': 'ENDORSEMENT';
    delete bcRec.species_group_id;
    delete bcRec.endorsement;
    return bcRec;
  }

  static async search(queryParams, db) {
    console.debug("Searching for:");
    console.debug(JSON.stringify(queryParams));
    let q = queryParams;
    let criteria = {};
    if (q.certType && q.certType == "ENDORSEMENT") criteria['endorsement is not'] = null;
    if (q.certType && q.certType == "SPECIES_GROUP_ID") criteria['species_group_id is not'] = null;
    
    if(q.before) criteria['valid_from_timestamp < '] = q.before; 
    if(q.after) criteria['valid_from_timestamp > '] = q.after; 

    if (q.competencyLevel) criteria.competency_level = q.competencyLevel;
    if (q.certification) {
      if (parseInt(q.certification) == q.certification) {
        criteria.species_group_id = parseInt(q.certification);             // this is common logic
      } else {
        criteria.endorsement = q.certification;
      }
    }
    if (q.banderId) criteria.bander_id = q.banderId;
    if (q.certificationCommentContains) criteria['certification_comment like '] = `%${q.certificationCommentContains}%`;

    console.log("Searching using these criteria:");
    console.log(criteria)
    let resultSet = await db.bander_certifications.find(criteria, BanderCertifications.returnFields);

    return resultSet.map(BanderCertifications.convertDBFieldsToCert);
  }

  static async delete(banderCertificationsId, db){
    console.debug("Deleting..." + banderCertificationsId);
    try{
      let result = await db.bander_certifications.destroy(banderCertificationsId);
      let recordActionParams = [
        {
          db_action: 'DELETE',
          db_table: 'bander_certifications',
          db_table_identifier_name: 'id',
          db_table_identifier_value: banderCertificationsId
        }
      ];
      let recordActionUpdate = await db.record_action.insert(recordActionParams);
      return result;
    } catch(e){
        console.error(e)
        throw e;
    }
  }
}

BanderCertifications.returnFields = {fields: ["species_group_id", "valid_from_timestamp", "valid_to_timestamp", 
      "competency_level", "certification_comment", "endorsement", "bander_id", "id"]}

module.exports = BanderCertifications;
 