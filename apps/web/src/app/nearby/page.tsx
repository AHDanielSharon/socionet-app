'use client';
import AppLayout from '@/components/layout/AppLayout';
import { MapPin, Navigation, Users } from 'lucide-react';

export default function NearbyPage() {
  return (
    <AppLayout>
      <div className="max-w-3xl mx-auto px-4 py-6">
        <div className="flex items-center gap-3 mb-6"><MapPin size={22} className="text-accent"/><div><h1 className="font-display font-black text-2xl">Nearby</h1><p className="text-text-3 text-sm">Discover people and events near you</p></div></div>
        <div className="text-center py-20">
          <Navigation size={56} className="mx-auto mb-4 text-text-4"/>
          <h3 className="font-display font-bold text-xl mb-2">Location Required</h3>
          <p className="text-text-3 text-sm mb-6 max-w-sm mx-auto">Enable location access to discover people and events near you. Your location is never shared publicly.</p>
          <button className="btn-primary" onClick={() => navigator.geolocation?.getCurrentPosition(() => {}, () => {})}>Enable Location →</button>
        </div>
      </div>
    </AppLayout>
  );
}
