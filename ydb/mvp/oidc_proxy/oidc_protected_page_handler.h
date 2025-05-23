#pragma once

#include "oidc_settings.h"
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http_proxy.h>

namespace NMVP::NOIDC {

class TProtectedPageHandler : public NActors::TActor<TProtectedPageHandler> {
    using TBase = NActors::TActor<TProtectedPageHandler>;

    const NActors::TActorId HttpProxyId;
    const TOpenIdConnectSettings Settings;

public:
    TProtectedPageHandler(const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event);

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
            cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

} // NMVP::NOIDC
