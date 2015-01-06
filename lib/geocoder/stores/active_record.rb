# -*- coding: utf-8 -*-
require 'geocoder/sql'
require 'geocoder/stores/base'

##
# Add geocoding functionality to any ActiveRecord object.
#
module Geocoder::Store
  module ActiveRecord
    include Base

    ##
    # Implementation of 'included' hook method.
    #
    def self.included(base)
      base.extend ClassMethods
      #Setup dynamic methods

      #If prefixes is blank, then we get the usual methods
      #otherwise we loop through the multiple prefixes to create all the methods for each scope

      lat = base.geocoder_options[:latitude]
      lng = base.geocoder_options[:longitude]

      base.geocoder_options[:prefixes].each do |pre|
        _pre = pre.empty? ? '' : "_#{pre}"
        pre_ = pre.empty? ? '' : "#{pre}_"

        base.class_eval %Q{
          scope :#{pre_}geocoded, lambda {
            where("#{pre_}#{lat} IS NOT NULL AND #{pre_}#{lng} IS NOT NULL")
          }
        }

        base.class_eval %Q{
          scope :#{pre_}not_geocoded, lambda {
            where("#{pre_}#{lat} IS NULL AND #{pre_}#{lng} IS NULL")
          }
        }

        base.class_eval %Q{
          scope :near#{_pre}, lambda {|location, *args|
            latitude, longitude = Geocoder::Calculations.extract_coordinates(location)
            if Geocoder::Calculations.coordinates_present?(latitude, longitude)
              options = near_scope_options("#{pre}", latitude, longitude, *args)
              select(options[:select]).where(options[:conditions]).
              order(options[:order])
            else
              select(select_clause(nil, "NULL", "NULL")).where(false_condition)
            end
          }
        }

        base.class_eval %Q{
          scope :#{pre_}within_bounding_box, lambda{ |bounds|
            sw_lat, sw_lng, ne_lat, ne_lng = bounds.flatten if bounds
            if sw_lat && sw_lng && ne_lat && ne_lng
              where(Geocoder::Sql.within_bounding_box(
                sw_lat, sw_lng, ne_lat, ne_lng,
                full_column_name(geocoder_options[:latitude]),
                full_column_name(geocoder_options[:longitude])
              ))
            else
              select(select_clause(nil, "NULL", "NULL")).where(false_condition)
            end
          }
        }
      end
    end

    ##
    # Methods which will be class methods of the including class.
    #
    module ClassMethods

      def distance_from_sql(location, *args)
        latitude, longitude = Geocoder::Calculations.extract_coordinates(location)
        if Geocoder::Calculations.coordinates_present?(latitude, longitude)
          distance_sql(latitude, longitude, *args)
        end
      end

      private # ----------------------------------------------------------------

      ##
      # Get options hash suitable for passing to ActiveRecord.find to get
      # records within a radius (in kilometers) of the given point.
      # Options hash may include:
      #
      # * +:units+   - <tt>:mi</tt> or <tt>:km</tt>; to be used.
      #   for interpreting radius as well as the +distance+ attribute which
      #   is added to each found nearby object.
      #   Use Geocoder.configure[:units] to configure default units.
      # * +:bearing+ - <tt>:linear</tt> or <tt>:spherical</tt>.
      #   the method to be used for calculating the bearing (direction)
      #   between the given point and each found nearby point;
      #   set to false for no bearing calculation. Use
      #   Geocoder.configure[:distances] to configure default calculation method.
      # * +:select+          - string with the SELECT SQL fragment (e.g. “id, name”)
      # * +:select_distance+ - whether to include the distance alias in the
      #                        SELECT SQL fragment (e.g. <formula> AS distance)
      # * +:select_bearing+  - like +:select_distance+ but for bearing.
      # * +:order+           - column(s) for ORDER BY SQL clause; default is distance;
      #                        set to false or nil to omit the ORDER BY clause
      # * +:exclude+         - an object to exclude (used by the +nearbys+ method)
      # * +:distance_column+ - used to set the column name of the calculated distance.
      # * +:bearing_column+  - used to set the column name of the calculated bearing.
      # * +:min_radius+      - the value to use as the minimum radius.
      #                        ignored if database is sqlite.
      #                        default is 0.0
      #
      def near_scope_options(prefix, latitude, longitude, radius = 20, options = {})
        prefix = prefix.to_s.empty? ? '' : "#{prefix}_"
        if options[:units]
          options[:units] = options[:units].to_sym
        end
        latitude_column = prefix + geocoder_options[:latitude].to_s
        longitude_column = prefix + geocoder_options[:longitude].to_s 
        options[:units] ||= (geocoder_options[:units] || Geocoder.config.units)
        select_distance = options.fetch(:select_distance)  { true }
        options[:order] = "" if !select_distance && !options.include?(:order)
        select_bearing = options.fetch(:select_bearing) { true }
        bearing = bearing_sql(prefix, latitude, longitude, options)
        distance = distance_sql(prefix, latitude, longitude, options)
        distance_column = prefix + options.fetch(:distance_column, 'distance')
        bearing_column = prefix + options.fetch(:bearing_column, 'bearing')


        b = Geocoder::Calculations.bounding_box([latitude, longitude], radius, options)
        args = b + [
          full_column_name(latitude_column),
          full_column_name(longitude_column)
        ]
        bounding_box_conditions = Geocoder::Sql.within_bounding_box(*args)

        if using_sqlite?
          conditions = bounding_box_conditions
        else
          min_radius = options.fetch(:min_radius, 0).to_f
          conditions = [bounding_box_conditions + " AND (#{distance}) BETWEEN ? AND ?", min_radius, radius]
        end
        {
          :select => select_clause(options[:select],
                                   select_distance ? distance : nil,
                                   select_bearing ? bearing : nil,
                                   distance_column,
                                   bearing_column),
          :conditions => add_exclude_condition(conditions, options[:exclude]),
          :order => options.include?(:order) ? options[:order] : "#{distance_column} ASC"
        }
      end

      ##
      # SQL for calculating distance based on the current database's
      # capabilities (trig functions?).
      #
      def distance_sql(prefix, latitude, longitude, options = {})
        method_prefix = using_sqlite? ? "approx" : "full"
        Geocoder::Sql.send(
          method_prefix + "_distance",
          latitude, longitude,
          full_column_name(prefix + geocoder_options[:latitude].to_s),
          full_column_name(prefix + geocoder_options[:longitude].to_s),
          options
        )
      end

      ##
      # SQL for calculating bearing based on the current database's
      # capabilities (trig functions?).
      #
      def bearing_sql(prefix, latitude, longitude, options = {})
        if !options.include?(:bearing)
          options[:bearing] = Geocoder.config.distances
        end
        if options[:bearing]
          method_prefix = using_sqlite? ? "approx" : "full"
          Geocoder::Sql.send(
            method_prefix + "_bearing",
            latitude, longitude,
            full_column_name(prefix + geocoder_options[:latitude].to_s),
            full_column_name(prefix + geocoder_options[:longitude].to_s),
            options
          )
        end
      end

      ##
      # Generate the SELECT clause.
      #
      def select_clause(columns, distance = nil, bearing = nil, distance_column = 'distance', bearing_column = 'bearing')
        if columns == :id_only
          return full_column_name(primary_key)
        elsif columns == :geo_only
          clause = ""
        else
          clause = (columns || full_column_name("*"))
        end
        if distance
          clause += ", " unless clause.empty?
          clause += "#{distance} AS #{distance_column}"
        end
        if bearing
          clause += ", " unless clause.empty?
          clause += "#{bearing} AS #{bearing_column}"
        end
        clause
      end

      ##
      # Adds a condition to exclude a given object by ID.
      # Expects conditions as an array or string. Returns array.
      #
      def add_exclude_condition(conditions, exclude)
        conditions = [conditions] if conditions.is_a?(String)
        if exclude
          conditions[0] << " AND #{full_column_name(primary_key)} != ?"
          conditions << exclude.id
        end
        conditions
      end

      def using_sqlite?
        connection.adapter_name.match(/sqlite/i)
      end

      def using_postgres?
        connection.adapter_name.match(/postgres/i)
      end

      ##
      # Use OID type when running in PosgreSQL
      #
      def null_value
        using_postgres? ? 'NULL::text' : 'NULL'
      end

      ##
      # Value which can be passed to where() to produce no results.
      #
      def false_condition
        using_sqlite? ? 0 : "false"
      end

      ##
      # Prepend table name if column name doesn't already contain one.
      #
      def full_column_name(column)
        column = column.to_s
        column.include?(".") ? column : [table_name, column].join(".")
      end
    end

    ##
    # Look up coordinates and assign to +latitude+ and +longitude+ attributes
    # (or other as specified in +geocoded_by+). Returns coordinates (array).
    #
    def geocode
      coords = self.class.geocoder_options[:prefixes].map do |pre|
        pre = pre.empty? ? pre : "#{pre}_"
        address_attr = "#{pre}#{self.class.geocoder_options[:address].to_s}"
        next if address_attr.nil? || address_attr.empty?

        address = self.public_send(address_attr)
        do_lookup(false, address) do |o,rs|
          if r = rs.first
            unless r.latitude.nil? or r.longitude.nil?
              o.__send__  "#{pre}#{self.class.geocoder_options[:latitude].to_s}=",  r.latitude
              o.__send__  "#{pre}#{self.class.geocoder_options[:longitude].to_s}=", r.longitude
            end
            r.coordinates
          end
        end
      end
      coords.size == 1 ? coords.flatten : coords
    end

    alias_method :fetch_coordinates, :geocode

    ##
    # Look up address and assign to +address+ attribute (or other as specified
    # in +reverse_geocoded_by+). Returns address (string).
    #
    def reverse_geocode
      do_lookup(true) do |o,rs|
        if r = rs.first
          unless r.address.nil?
            o.__send__ "#{self.class.geocoder_options[:fetched_address]}=", r.address
          end
          r.address
        end
      end
    end

    alias_method :fetch_address, :reverse_geocode
  end
end
