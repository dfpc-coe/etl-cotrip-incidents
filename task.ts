import moment from 'moment-timezone';
import { Static, Type, TSchema } from '@sinclair/typebox';
import { FeatureCollection, Feature, Geometry } from 'geojson';
import ETL, { Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';

export default class Task extends ETL {
    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'COTRIP_TOKEN': Type.String({ description: 'API Token for CoTrip' }),
                'Point Geometries': Type.Boolean({ description: 'Allow point geometries', default: true }),
                'LineString Geometries': Type.Boolean({ description: 'Allow LineString geometries', default: true }),
                'Polygon Geometries': Type.Boolean({ description: 'Allow Polygon Geometries', default: true }),
                'DEBUG': Type.Boolean({ description: 'Print GeoJSON Features in logs', default: false, })
            });
        } else {
            return Type.Object({
                incident_type: Type.String(),
                incident_status: Type.String(),
                startTime: Type.String(),
                lastUpdated: Type.String(),
                travelerInformationMessage: Type.String(),
            });
        }
    }

    async control() {
        const layer = await this.fetchLayer();

        const api = 'https://data.cotrip.org/';
        if (!layer.environment.COTRIP_TOKEN) throw new Error('No COTrip API Token Provided');
        const token = layer.environment.COTRIP_TOKEN;

        const incidents: Feature[] = [];
        let batch = -1;
        let res;
        do {
            console.log(`ok - fetching ${++batch} of incidents`);
            const url = new URL('/api/v1/incidents', api);
            url.searchParams.append('apiKey', String(token));
            if (res) url.searchParams.append('offset', res.headers.get('next-offset'));

            res = await fetch(url);

            incidents.push(...(await res.json()).features);
        } while (res.headers.has('next-offset') && res.headers.get('next-offset') !== 'None');
        console.log(`ok - fetched ${incidents.length} incidents`);

        const features: Feature[] = [];
        for (const feature of incidents.map((incident) => {
            return {
                id: incident.properties.id,
                type: 'Feature',
                properties: {
                    remarks: incident.properties.travelerInformationMessage,
                    callsign: incident.properties.type,
                    type: 'a-f-G',
                    metadata: {
                        incident_type: incident.properties.type,
                        incident_status: incident.properties.status,
                        startTime: moment(incident.properties.startTime).tz('America/Denver').format('YYYY-MM-DD HH:mm z'),
                        lastUpdated: moment(incident.properties.lastUpdated).tz('America/Denver').format('YYYY-MM-DD HH:mm z'),
                        travelerInformationMessage: incident.properties.travelerInformationMessage
                    }
                },
                geometry: incident.geometry
            } as Feature;
        })) {
            if (feature.geometry.type.startsWith('Multi')) {
                const feat = JSON.stringify(feature);
                const type = feature.geometry.type.replace('Multi', '');

                let i = 0;
                // @ts-ignore GeomCollect has no geometry coordinates
                for (const coordinates of feature.geometry.coordinates) {
                    const new_feat = JSON.parse(feat);
                    new_feat.geometry = { type, coordinates };
                    new_feat.id = new_feat.id + '-' + i;
                    features.push(new_feat);
                    ++i;
                }
            } else {
                features.push(feature);
            }
        }

        const allowed: string[] = [];
        if (layer.environment['Point Geometries']) allowed.push('Point');
        if (layer.environment['LineString Geometries']) allowed.push('LineString');
        if (layer.environment['Polygon Geometries']) allowed.push('Polygon');

        const fc: FeatureCollection = {
            type: 'FeatureCollection',
            features: features.filter((feat) => {
                return allowed.includes(feat.geometry.type);
            })
        };

        await this.submit(fc);
    }
}

env(import.meta.url)
await local(new Task(), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(), event);
}
