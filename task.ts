import moment from 'moment-timezone';
import type { Feature } from 'geojson';
import { Static, Type, TSchema } from '@sinclair/typebox';
import ETL, { InputFeatureCollection, InputFeature, Event, SchemaType, handler as internal, local, env } from '@tak-ps/etl';

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
                status: Type.String(),
                direction: Type.Number(),
                routeName: Type.String(),
                severity: Type.String(),
                responseLevel: Type.String(),
                category: Type.String(),
                startTime: Type.String(),
                startMarker: Type.Optional(Type.Number()),
                endMarker: Type.Optional(Type.Number()),
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

        const features: Static<typeof InputFeature>[] = [];
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
                        status: incident.properties.status,
                        direction: incident.properties.direction,
                        routeName: incident.properties.routeName,
                        severity: incident.properties.severity,
                        responseLevel: incident.properties.responseLevel,
                        category: incident.properties.category,
                        startMarker: incident.properties.startMarker,
                        endMarker: incident.properties.endMarker,
                        startTime: moment(incident.properties.startTime).tz('America/Denver').format('YYYY-MM-DD HH:mm z'),
                        lastUpdated: moment(incident.properties.lastUpdated).tz('America/Denver').format('YYYY-MM-DD HH:mm z'),
                        travelerInformationMessage: incident.properties.travelerInformationMessage
                    }
                },
                geometry: incident.geometry
            } as Static<typeof InputFeature>;
        })) {
            if (feature.geometry.type.startsWith('Multi')) {
                const feat = JSON.stringify(feature);
                const type = feature.geometry.type.replace('Multi', '');

                let i = 0;

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

        const fc: Static<typeof InputFeatureCollection> = {
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
