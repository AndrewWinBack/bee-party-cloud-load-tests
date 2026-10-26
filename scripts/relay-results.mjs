import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import assert from 'node:assert/strict';
const repo=process.env.GITHUB_REPOSITORY,run=process.env.SOURCE_RUN_ID,token=process.env.GH_TOKEN;
assert.equal(repo,'AndrewWinBack/bee-party-cloud-load-tests');assert.match(run||'',/^\d+$/);
execFileSync('gh',['run','download',run,'--repo',repo,'--dir','relay'],{stdio:'inherit',timeout:180000});
const api=async(p,body)=>{const r=await fetch('https://api.github.com/repos/'+repo+p,{method:body?'POST':'GET',headers:{Authorization:'Bearer '+token,Accept:'application/vnd.github+json','Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});if(!r.ok)throw Error('GitHub API '+r.status);return r.json()};
const entries=[];
for(const name of fs.readdirSync('relay')){
 const file=path.join('relay',name,'results.enc.json'),content=fs.readFileSync(file),data=JSON.parse(content);assert.deepEqual(Object.keys(data).sort(),['data','iv','tag']);
 const blob=await api('/git/blobs',{encoding:'base64',content:content.toString('base64')});entries.push({path:name+'.enc.json',mode:'100644',type:'blob',sha:blob.sha});
}
assert.equal(entries.length,4);
const tree=await api('/git/trees',{tree:entries}),commit=await api('/git/commits',{message:'Encrypted evidence for run '+run,tree:tree.sha,parents:[]});
await api('/git/refs',{ref:'refs/heads/evidence-'+run,sha:commit.sha});
console.log(JSON.stringify({run,files:4,branch:'evidence-'+run,encryptedOnly:true}));
