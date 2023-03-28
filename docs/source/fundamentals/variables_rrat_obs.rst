.. highlight: yml
.. _variables_rrat:

rrat-observation-sets.yml
#########################




Content of ``rrat-observation.sets.yml``::

    run.node:
        baker:
            obs: L1
            ncpu: 64
        muddy:
            obs: L2
            ncpu: 64
        clapton:
            obs: L3
            ncpu: 64
        bruce:
            obs: U1
            ncpu: 64

    obs:
        L1:
            ms: ../msdir/1622491578_sdp_l0-J2009_2026-corr.ms   # L deep 1
            dirs.sub: obs-l1
            band: L
            minuv-l: 700  
            taper-inner-tukey: 100
        L2:
            ms: "../msdir/1624216341_sdp_l0-J2009_2026-corr.ms" # L deep 2
            dirs.sub: obs-l2
            band: L
            minuv-l: 700  
            taper-inner-tukey: 100
        L3:
            ms: "../msdir/1627405233_sdp_l0-J2009_2026-corr.ms" # L deep from U+L obs Jul 2021
            dirs.sub: obs-l3
            band: L
            minuv-l: =UNSET
            taper-inner-tukey: =UNSET
        U0:
            ms: ../msdir/1625623568_sdp_l0-J2009_2026-corr.ms  # UHF test obs Jul 2021 1h
            dirs.sub: obs-u0
            band: UHF
        U1:
            ms: ../msdir/1627405250_sdp_l0-J2009_2026-corr.ms  # UHF from U+L obs Jul 2021
            dirs.sub: obs-u1
            band: UHF
            minuv-l: =UNSET
            taper-inner-tukey: =UNSET
        U2:
            ms: ../msdir/1628439081_sdp_l0-J2009_2026-corr.ms  # UHF Aug 2021 9h
            dirs.sub: obs-u2
            band: UHF
            minuv-l: 250
            taper-inner-tukey: 100
            mad_flag: true
            flag-on-residuals: false
        U3:
            ms: ../msdir/1643947704_sdp_l0-J2009_2026-corr.ms # UHF Feb 2022  part I
            dirs.sub: obs-u3
            band: UHF
            minuv-l: 375 # 150m at 40cm
            taper-inner-tukey: 100
            mad_flag: true
            dd_selfcal-3.dE.time_interval: 8
            dd_selfcal-3.dE.freq_interval: 64
            dd_selfcal-4.dE.time_interval: 8
            dd_selfcal-4.dE.freq_interval: 64

        U3b:
            ms: ../msdir/1643969937_sdp_l0-J2009_2026-corr.ms # UHF Feb 2022 part II
            dirs.sub: obs-u3b
            band: UHF
            minuv-l: 375 # 150m at 40cm
            taper-inner-tukey: 100
            mad_flag: true
            dd_selfcal-3.dE.time_interval: 8
            dd_selfcal-3.dE.freq_interval: 64
            dd_selfcal-4.dE.time_interval: 8
            dd_selfcal-4.dE.freq_interval: 64

        U3c:
            ms: ../msdir/U3-combined.ms # UHF Feb 2022 combined
            dirs.sub: obs-u3c
            band: UHF
            minuv-l: 375 # 150m at 40cm
            taper-inner-tukey: 100
            mad_flag: true
            dd_selfcal-3.dE.time_interval: 8
            dd_selfcal-3.dE.freq_interval: 64
            dd_selfcal-4.dE.time_interval: 8
            dd_selfcal-4.dE.freq_interval: 64

    band:
        L:
            deep-mask-1: masks/lband-deep/im2-mask-13000.fits
            deep-mask-notarget-1: masks/lband-deep/im2-mask-nodwarf.fits
            deep-mask-2: masks/lband-deep/im2-mask.fits
            deep-mask-notarget-2: masks/lband-deep/im2-mask-nodwarf.fits
            deep-mask-3: masks/lband-deep/im2-mask.fits
            deep-mask-notarget-3: maskrecipe.htc_cadences/lband-deep/im2-mask-nodwarf.fits
            deep-mask-ddf: masks/lband-deep/im3-mask-10125.fits
            wsclean_size: 13000
            pixel_scale: 0.8
            htc_size: 3072
            htc_scale: 2.4arcsec
            wsclean_nchan: 8
            lib.steps.ddfacet.base.params.Image.Cell: 0.8
            psf_size: 6
            weight: "briggs 0"
            ddf-precluster-file: dd-regs-lband.reg
            initial-flag-version: cb_flag__target_rfi_after
            lightcurves-within: 0.5deg
            mdv-beams: beam/MeerKAT_L_band_primary_beam.npz
            power-beam: beam/MeerKAT_L_band_StokesBeam.I.fits
            catalog-flux-column: flux_L

        UHF:
            deep-mask-1: masks/uhf-deep/im2-mask-13000.fits
            deep-mask-notarget-1: masks/uhf-deep/im2-mask-nodwarf.fits
            deep-mask-2: masks/uhf-deep/im2-mask.fits
            deep-mask-notarget-2: masks/uhf-deep/im2-mask-nodwarf.fits
            deep-mask-3: masks/uhf-deep/im3-mask.fits
            deep-mask-notarget-3: masks/uhf-deep/im3-mask-nodwarf.fits
            deep-mask-ddf: masks/uhf-deep/im3-mask-10125.fits
            wsclean_size: 13000
            pixel_scale: 1.6
            htc_size: 3072
            htc_scale: 4.8arcsec
            htfc_size: 2048
            htfc_scale: 4.8arcsec
            htfc_nband: 8
            wsclean_nchan: 6
            lib.steps.ddfacet.base.params.Image.Cell: 1.6
            psf_size: 12
            weight: "briggs 0"
            ddf-precluster-file: dd-regs-uhf.reg
            initial-flag-version: cbuhf_flag__target_rfi_after
            lightcurves-within: 1deg
            mdv-beams: beam/MeerKAT_U_band_primary_beam.npz
            power-beam: beam/MeerKAT_U_band_StokesBeam.I.fits
            catalog-flux-column: flux_U



