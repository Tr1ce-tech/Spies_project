import fs from 'node:fs/promises';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const payload=JSON.parse(await fs.readFile('proceed_data/pattern_catalog_v1/catalog.json','utf8'));
const template=await fs.readFile('pipelines/pattern_catalog.html','utf8');
const scripts=[...template.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)];
const checks=Object.keys(payload.labels).map(value=>({value,checked:true}));
const nodes=new Map();let exported;
function node(id){if(!nodes.has(id))nodes.set(id,{value:'',textContent:'',innerHTML:'',hidden:true,
 addEventListener(){},scrollIntoView(){},querySelectorAll(){return checks.filter(x=>x.checked)}});return nodes.get(id);}
node('data').textContent=JSON.stringify(payload);node('kind').value='source_profile';node('mode').value='any';node('sort').value='flight_days';
const context={document:{getElementById:node,createElement(){return {click(){}}}},
 Blob,URL:{createObjectURL(blob){exported=blob;return 'local-test'},revokeObjectURL(){}},console};
vm.runInNewContext(scripts.at(-1)[1]+'\nglobalThis.api={update,show,getRows:()=>filtered};',context);
assert.equal(context.api.getRows().length,2386);
for(const check of checks)check.checked=check.value==='high_observed_frequency';
context.api.update();assert.equal(context.api.getRows().length,874);
node('export').onclick();assert.equal(JSON.parse(await exported.text()).length,874);
for(const check of checks)check.checked=check.value==='repeated_route';
context.api.update();assert.equal(context.api.getRows().length,0);
for(const check of checks)check.checked=true;
node('mode').value='all';context.api.update();assert.equal(context.api.getRows().length,0);
node('mode').value='any';node('kind').value='preliminary_group';context.api.update();
assert.equal(context.api.getRows().length,1009);
const row=context.api.getRows()[0];context.api.show(row.unit_id);
assert.equal(node('detail').hidden,false);assert.ok(node('detail').innerHTML.includes(row.unit_id));
node('query').value=row.unit_id;context.api.update();assert.equal(context.api.getRows().length,1);
assert.ok(payload.candidates.every(r=>r.iforest_score===undefined&&r.iforest_flag===undefined));
console.log('Offline logic tests passed: profile/group filters, OR/AND, empty repeat result, search, details, full export.');
