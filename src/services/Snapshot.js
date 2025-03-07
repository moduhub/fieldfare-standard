/**
 * Fieldfare: Backend framework for distributed networks
 *
 * Copyright 2021-2023 Adan Kvitschal
 * ISC LICENSE
 */

import { LocalService, Chunk, HostIdentifier, Utils, cryptoManager, logger} from '@fieldfare/core';

export const snapshotServiceUUID = 'd2b38791-51af-4767-ad08-2f9f1425e90e';

const snapshotServiceDefinition = {
    "uuid": snapshotServiceUUID,
    "name": "snapshot",
    "methods": [
        "store"
    ],
    "collection": [
        {
            "name": "hostStates",
            "descriptor": {
                "type": "map",
                "degree": 4
            }
        }
    ]
};

export {snapshotServiceUUID as uuid};
export {snapshotServiceDefinition as definition};
export {SnapshotService as implementation};

var gNumCalls=0;

export class SnapshotService extends LocalService {

    async store(remoteHost, stateMessage) {
        gNumCalls++;
        //1) Check if remoteHost belongs to the enviroment
        if(await this.environment.getServicesForHost(remoteHost) === false) {
            throw Error('host does not belong to the environment');
        }

        logger.debug('[STORE-C'+gNumCalls+'] Called by ' + remoteHost.id);
        //2) Check if provided state is valid
        try{
            Utils.validateParameters(stateMessage, ['service', 'data', 'ts', 'signature', 'source']);
            Utils.validateParameters(stateMessage.data, ['id', 'ts', 'collections']);
        } catch(e) {
            logger.debug('[STORE-C'+gNumCalls+'] Message validation failed: ' + e.message);
            throw Error(e.message);
        }
        const timeDelta = Date.now() - stateMessage.data.ts;
        if(timeDelta > 1000 * 60 * 60 * 24) {   //a day
            logger.debug('[STORE-C'+gNumCalls+'] Rejected: state is too old');
            throw Error('state is too old');
        }
        const signatureIsValid = await cryptoManager.verifyMessage(stateMessage, remoteHost.pubKey);
        if(!signatureIsValid) {
            logger.debug('[STORE-C'+gNumCalls+'] Rejected: sigature is invalid');
            throw Error('signature is invalid');
        }

        //3) Check if provided state is newer than previous
        const hostStates = await this.collection.getElement('hostStates');
        if(!hostStates) {
            logger.debug('[STORE-C'+gNumCalls+'] Internal error: hostStates is null');
            throw Error('hostStates is null');
        }
        const keyChunk = Chunk.fromIdentifier(HostIdentifier.toChunkIdentifier(remoteHost.id));
        const valueChunk = await hostStates.get(keyChunk);
        let prevState = null;
        let stateIsNewer = false;
        let stateIsDifferent = false;
        if(valueChunk) {
            prevState = await valueChunk.expand(0);
            if(prevState.data.ts < stateMessage.data.ts) {
                stateIsNewer = true;
            }
                for (const uuid in stateMessage.data.collections) {
                        let status;
                        if(uuid in prevState.data.collections === false) {
                                status = ' [new]';
                                stateIsDifferent=true;
                        } else
                        if(prevState.data.collections[uuid] !== stateMessage.data.collections[uuid]) {
                                stateIsDifferent = true;
                                status = ' [change]';
                        } else {
                                status = ' [stale]';
                        }
                        logger.debug('[STORE-C'+gNumCalls+']' + uuid + ' => ' + stateMessage.data.collections[uuid] + status);
                }
        }
        if((stateIsNewer && stateIsDifferent) || !prevState) {
            const stateChunk = await Chunk.fromObject(stateMessage);
            stateChunk.ownerID = remoteHost.id;
            //4) Clone complete stat
            try {
                const startTime = Date.now();
                const numClonedChunks = await stateChunk.clone();
                const elapsedTime = Date.now() - startTime;
                logger.debug('[STORE-C'+gNumCalls+'] SnapshotService.store() cloned ' + numClonedChunks + ' chunks in ' + elapsedTime + 'ms');
            } catch (error) {
                logger.debug('[STORE-C'+gNumCalls+'] SnapshotService.store() failed to clone state: ' + error.message);
                throw Error('SnapshotService.store() failed to clone state: ' + error.message);
            }
            //5) Store state as latest
            await hostStates.set(keyChunk, stateChunk);
            await this.collection.updateElement('hostStates', hostStates.descriptor);
            logger.debug('[STORE-C'+gNumCalls+'] state updated');
            return 'ok';
        }
        logger.debug('[STORE-C'+gNumCalls+'] state is stale');
        return 'state is stale';
    }

};